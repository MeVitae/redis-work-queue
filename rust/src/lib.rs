//! A work queue, on top of a redis database, with implementations in Python, Rust, Go and C#.
//!
//! This is the Rust implementations. For an overview of how the work queue works, it's
//! limitations, and the general concepts and implementations in other languages, please read the
//! [redis-work-queue readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md).
//!
//! ## Setup
//!
//! ```rust
//! # async fn example() -> redis::RedisResult<()> {
//! use redis_work_queue::{Item, KeyPrefix, WorkQueue};
//!
//! let host = "your-redis-server";
//! let db = &mut redis::Client::open(format!("redis://{host}/"))?
//!     .get_async_connection()
//!     .await?;
//!
//! let work_queue = WorkQueue::new(KeyPrefix::from("example_work_queue"));
//! # Ok(())
//! # }
//! ```
//!
//! ## Adding work
//!
//! ### Creating `Item`s
//!
//! ```rust
//! use redis_work_queue::Item;
//!
//! // Create an item from `Box<[u8]>`
//! let box_item = Item::new(Box::new(*b"[1,2,3]"));
//!
//! // Create an item from a `String`
//! let string_item = Item::from_string_data("[1,2,3]".to_string());
//!
//! // Create an item from a serializable type
//! let json_item = Item::from_json_data(&[1, 2, 3]).unwrap();
//!
//! assert_eq!(box_item.data, string_item.data);
//! assert_eq!(box_item.data, json_item.data);
//!
//! // Parse an Item's data as json:
//! assert_eq!(box_item.data_json::<Vec<u32>>().unwrap(), vec![1, 2, 3]);
//! ```
//!
//! ### Add an item to a work queue
//! ```rust
//! # use redis::{AsyncCommands, RedisResult};
//! # use redis_work_queue::{Item, KeyPrefix, WorkQueue};
//! # async fn add_item<C: AsyncCommands>(db: &mut C, work_queue: WorkQueue, item: Item) -> redis::RedisResult<()> {
//! work_queue.add_item(db, &item).await.expect("failed to add item to work queue");
//! # Ok(())
//! # }
//! ```
//!
//! ## Completing work
//!
//! Please read [the documentation on leasing and completing
//! items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).
//!
//! ```rust
//! use std::time::Duration;
//!
//! use redis::{AsyncCommands, RedisResult};
//! use redis_work_queue::{Item, WorkQueue};
//!
//! # fn do_some_work(_: &Item) {}
//! pub async fn work_loop<C: AsyncCommands>(db: &mut C, work_queue: WorkQueue) -> RedisResult<()> {
//!     loop {
//!         // Wait for a job with no timeout and a lease time of 5 seconds.
//!         let job: Item = work_queue.lease(db, None, Duration::from_secs(5)).await?.unwrap();
//!         do_some_work(&job);
//!         work_queue.complete(db, &job);
//!     }
//! }
//! ```
//!
//! ### Handling errors
//!
//! Please read [the documentation on handling
//! errors](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#handling-errors).
//!
//! ```rust
//! use std::time::Duration;
//!
//! use redis::{AsyncCommands, RedisResult};
//! use redis_work_queue::{Item, WorkQueue};
//!
//! # struct ExampleError {
//! #     should_retry: bool,
//! # }
//! # impl ExampleError {
//! #     fn should_retry(&self) -> bool {
//! #         self.should_retry
//! #     }
//! # }
//! # fn do_some_work(_: &Item) -> Result<(), ExampleError> { Ok(()) }
//! # fn log_error(_: ExampleError) {}
//! pub async fn work_loop<C: AsyncCommands>(db: &mut C, work_queue: WorkQueue) -> RedisResult<()> {
//!     loop {
//!         // Wait for a job with no timeout and a lease time of 5 seconds.
//!         let job: Item = work_queue.lease(db, None, Duration::from_secs(5)).await?.unwrap();
//!         match do_some_work(&job) {
//!             // Mark successful jobs as complete
//!             Ok(()) => {
//!                 work_queue.complete(db, &job).await?;
//!             }
//!             // Drop a job that should be retried - it will be returned to the work queue after
//!             // the (5 second) lease expires.
//!             Err(err) if err.should_retry() => (),
//!             // Errors that shouldn't cause a retry should mark the job as complete so it isn't
//!             // tried again.
//!             Err(err) => {
//!                 log_error(err);
//!                 work_queue.complete(db, &job).await?;
//!             }
//!         }
//!     }
//! }
//! ```

use std::future::Future;
use std::time::Duration;

use redis::{AsyncCommands, RedisResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A string which should be prefixed to an identifier to generate a database key.
///
/// ### Example
///
/// ```rust
/// use redis_work_queue::KeyPrefix;
///
/// let cv_key = KeyPrefix::from("cv:");
/// // ...
/// let cv_id = "abcdef-123456";
/// assert_eq!(cv_key.of(cv_id), "cv:abcdef-123456");
/// // let cv_info = db.get(cv_key.of(cv_id));
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KeyPrefix {
    prefix: String,
}

impl KeyPrefix {
    pub fn new(prefix: String) -> KeyPrefix {
        KeyPrefix { prefix }
    }

    /// Returns the result of prefixing `self` onto `name`.
    pub fn of(&self, name: &str) -> String {
        let mut key = String::with_capacity(self.prefix.len() + name.len());
        key.push_str(&self.prefix);
        key.push_str(name);
        key
    }

    /// Returns the result of prefixing `self` onto `other` as a new `KeyPrefix`.
    ///
    /// This is like [`KeyPrefix::concat`] except it only borrows `self`.
    pub fn and(&self, other: &str) -> KeyPrefix {
        KeyPrefix::new(self.of(other))
    }

    /// Returns the result of prefixing `self` onto `other` as a new `KeyPrefix`.
    ///
    /// This is like [`KeyPrefix::and`] except it moves `self`.
    pub fn concat(mut self, other: &str) -> KeyPrefix {
        self.prefix.push_str(other);
        self
    }
}

impl From<String> for KeyPrefix {
    fn from(prefix: String) -> KeyPrefix {
        KeyPrefix::new(prefix)
    }
}

impl From<&str> for KeyPrefix {
    fn from(prefix: &str) -> KeyPrefix {
        KeyPrefix::new(prefix.to_string())
    }
}

impl From<KeyPrefix> for String {
    fn from(key_prefix: KeyPrefix) -> String {
        key_prefix.prefix
    }
}

impl AsRef<str> for KeyPrefix {
    fn as_ref(&self) -> &str {
        &self.prefix
    }
}

/// An item for a work queue. Each item has an ID and associated data.
#[derive(Clone, Debug)]
pub struct Item {
    pub id: String,
    pub data: Box<[u8]>,
}

impl Item {
    /// Create a new item, with the provided `data` and a random id (a uuid).
    pub fn new(data: Box<[u8]>) -> Item {
        Item {
            data,
            id: Uuid::new_v4().to_string(),
        }
    }

    /// Create a new item, with the provided `data` and a random id (a uuid).
    ///
    /// The item's data is the output of `data.into_bytes().into_boxed_slice()`.
    pub fn from_string_data(data: String) -> Item {
        Item::new(data.into_bytes().into_boxed_slice())
    }

    /// Create a new item with a random id (a uuid). The data is the result of
    /// `serde_json::to_vec(data)`.
    pub fn from_json_data<T: Serialize>(data: &T) -> serde_json::Result<Item> {
        Ok(Item::new(serde_json::to_vec(data)?.into()))
    }

    /// Returns the data, parsed as JSON.
    pub fn data_json<'a, T: Deserialize<'a>>(&'a self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.data)
    }

    /// Returns the data, parsed as JSON, with a static lifetime.
    pub fn data_json_static<T: for<'de> Deserialize<'de>>(&self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.data)
    }
}

/// A work queue backed by a redis database
pub struct WorkQueue {
    /// A unique ID for this instance
    session: String,
    /// The key for the list of items in the queue
    main_queue_key: String,
    /// The key for the list of items being processed
    processing_key: String,
    // TODO: Implement cleaning in the Rust library?
    //cleaning_key: String,
    /// The key prefix for lease entries
    lease_key: KeyPrefix,
    /// The key for item data entries
    item_data_key: KeyPrefix,
}

impl WorkQueue {
    pub fn new(name: KeyPrefix) -> WorkQueue {
        WorkQueue {
            session: Uuid::new_v4().to_string(),
            main_queue_key: name.of(":queue"),
            processing_key: name.of(":processing"),
            //cleaning_key: name.of(":cleaning"),
            lease_key: name.and(":leased_by_session:"),
            item_data_key: name.and(":item:"),
        }
    }

    /// Add an item to the work queue. This adds the redis commands onto the pipeline passed.
    ///
    /// Use [`WorkQueue::add_item`] if you don't want to pass a pipeline directly.
    pub fn add_item_to_pipeline(&self, pipeline: &mut redis::Pipeline, item: &Item) {
        // Add the item data
        // NOTE: it's important that the data is added first, otherwise someone could pop the item
        // before the data is ready
        pipeline.set(self.item_data_key.of(&item.id), item.data.as_ref());
        // Then add the id to the work queue
        pipeline.lpush(&self.main_queue_key, &item.id);
    }

    /// Add an item to the work queue.
    ///
    /// This creates a pipeline and executes it on the database.
    pub async fn add_item<C: AsyncCommands>(&self, db: &mut C, item: &Item) -> RedisResult<()> {
        let mut pipeline = Box::new(redis::pipe());
        self.add_item_to_pipeline(&mut pipeline, item);
        pipeline.query_async(db).await
    }

    /// Return the length of the work queue (not including items being processed, see
    /// [`WorkQueue::processing`]).
    pub fn queue_len<'a, C: AsyncCommands>(
        &'a self,
        db: &'a mut C,
    ) -> impl Future<Output = RedisResult<usize>> + 'a {
        db.llen(&self.main_queue_key)
    }

    /// Return the number of items being processed.
    pub fn processing<'a, C: AsyncCommands>(
        &'a self,
        db: &'a mut C,
    ) -> impl Future<Output = RedisResult<usize>> + 'a {
        db.llen(&self.processing_key)
    }

    pub async fn get_queue_lengths<'a, C: AsyncCommands>(
        &'a self,
        db: &mut C,
    ) -> RedisResult<(u32,u32)> {
        let (queue_length, processing_length): (u32, u32) = redis::pipe()
            .atomic()
            .llen(&self.main_queue_key)
            .llen(&self.processing_key)
            .query_async(db)
            .await?;

        Ok((queue_length, processing_length))
    }

    /// Request a work lease the work queue. This should be called by a worker to get work to
    /// complete. When completed, the `complete` method should be called.
    ///
    /// The function will return either when a job is leased or after `timeout` if `timeout`
    /// isn't `None`.
    ///
    /// If the job is not completed (by calling [`WorkQueue::complete`]) before the end of
    /// `lease_duration`, another worker may pick up the same job. It is not a problem if a job is
    /// marked as `done` more than once.
    ///
    /// If you've not already done it, it's worth reading [the documentation on leasing
    /// items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).
    pub async fn lease<C: AsyncCommands>(
        &self,
        db: &mut C,
        timeout: Option<Duration>,
        lease_duration: Duration,
    ) -> RedisResult<Option<Item>> {
        // First, to get an item, we try to move an item from the main queue to the processing list.
        let item_id: Option<String> = match timeout {
            Some(Duration::ZERO) => {
                db.rpoplpush(&self.main_queue_key, &self.processing_key)
                    .await?
            }
            _ => {
                db.brpoplpush(
                    &self.main_queue_key,
                    &self.processing_key,
                    timeout.map(|d| d.as_secs() as usize).unwrap_or(0),
                )
                .await?
            }
        };

        // If we got an item, fetch the associated data.
        let item = match item_id {
            Some(item_id) => Item {
                data: db
                    .get::<_, Vec<u8>>(self.item_data_key.of(&item_id))
                    .await?
                    .into_boxed_slice(),
                id: item_id,
            },
            None => return Ok(None),
        };

        // Now setup the lease item.
        // NOTE: Racing for a lease is ok
        db.set_ex(
            self.lease_key.of(&item.id),
            &self.session,
            lease_duration.as_secs() as usize,
        )
        .await?;

        Ok(Some(item))
    }

    /// Marks a job as completed and remove it from the work queue. After `complete` has been called
    /// (and returns `true`), no workers will receive this job again.
    ///
    /// `complete` returns a boolean indicating if *the job has been removed* **and** *this worker
    /// was the first worker to call `complete`*. So, while lease might give the same job to
    /// multiple workers, complete will return `true` for only one worker.
    pub async fn complete<C: AsyncCommands>(&self, db: &mut C, item: &Item) -> RedisResult<bool> {
        let removed: usize = db.lrem(&self.processing_key, 0, &item.id).await?;
        if removed == 0 {
            return Ok(false);
        }
        // If we did actually remove it, delete the item data and lease.
        // If we didn't really remove it, it's probably been returned to the work queue so the
        // data is still needed and the lease might not be ours (if it is still ours, it'll
        // expire anyway).
        redis::pipe()
            .del(self.item_data_key.of(&item.id))
            .del(self.lease_key.of(&item.id))
            .query_async(db)
            .await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{Item, KeyPrefix};

    use serde::{Deserialize, Serialize};

    #[test]
    fn test_key_prefix() {
        let prefix = KeyPrefix::new("abc".to_string());
        let another_prefix = prefix.and("123");
        let final_prefix = KeyPrefix::new("abc123".to_string());
        assert_eq!(another_prefix, final_prefix);
        assert_ne!(prefix, another_prefix);
        assert_eq!(another_prefix.as_ref(), final_prefix.as_ref());
        assert_eq!(prefix.as_ref(), "abc");
        assert_eq!(prefix.of("bar"), "abcbar");
        assert_eq!(
            Into::<String>::into(prefix.and("foo")),
            "abcfoo".to_string()
        );
        assert_eq!(prefix.of("foo"), "abcfoo".to_string());
        assert_eq!(prefix.and("foo").of("bar"), "abcfoobar".to_string());
    }

    #[test]
    fn test_item_json() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct Test {
            #[serde(default)]
            n: usize,
            s: String,
        }

        let test_foo = Test {
            n: 7,
            s: "foo".to_string(),
        };
        let test_bar = Test {
            n: 8,
            s: "bar".to_string(),
        };
        let test_baz = Test {
            n: 0,
            s: "baz".to_string(),
        };

        assert_eq!(
            test_foo,
            Item::from_json_data(&test_foo)
                .unwrap()
                .data_json()
                .unwrap()
        );

        let test_item_bar = Item::from_json_data(&test_bar).unwrap();
        assert_eq!(
            test_item_bar.id.len(),
            "00112233-4455-6677-8899-aabbccddeeff".len()
        );
        let test_item_baz = Item::new(
            "{\"s\":\"baz\"}"
                .to_string()
                .into_bytes()
                .into_boxed_slice(),
        );
        assert_eq!(
            test_item_baz.id.len(),
            "00112233-4455-6677-8899-aabbccddeeff".len()
        );
        assert_ne!(test_item_bar.id, test_item_baz.id);
        assert_ne!(test_item_bar.data, test_item_baz.data);
        assert_ne!(
            test_item_bar.data_json::<Test>().unwrap(),
            test_item_baz.data_json().unwrap()
        );
        assert_eq!(
            test_item_bar.data_json::<Test>().unwrap(),
            test_item_bar.data_json().unwrap()
        );
        assert_eq!(test_item_bar.data_json::<Test>().unwrap(), test_bar);
        assert_eq!(test_item_baz.data_json::<Test>().unwrap(), test_baz);
    }
}
