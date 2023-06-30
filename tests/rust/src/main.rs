use std::time::Duration;

use futures_lite::future;
use redis::{AsyncCommands, RedisResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;


use redis_work_queue::{Item, KeyPrefix, WorkQueue};
use rand::Rng;

#[derive(Serialize, Deserialize)]
struct SharedJobData {
    a: i32,
    b: i32,
}

#[derive(Serialize)]
struct SharedJobResult {
    a: i32,
    sum: i32,
    prod: i32,
    worker: String,
}

fn main() -> RedisResult<()> {
    future::block_on(async_main())
}


async fn run_duplicate_items_test(db: &mut redis::aio::Connection) -> Result<(), redis::RedisError> {
    let key_prefix = KeyPrefix::new("Duplicate-Items-Test".to_string());

    let work_queue1 = WorkQueue::new(key_prefix.clone());
    let mut passed = 0;
    let mut not_passed = 0;
    let number_of_tests = 150;
    let number_of_random_possible_items = 232;
    let mut items = Vec::new();

    for _ in 1..=number_of_tests {
        let item_data1 = rand::thread_rng().gen_range(0..number_of_random_possible_items);
        let item_data2 = rand::thread_rng().gen_range(0..number_of_random_possible_items);
        let item1 = Item { id: item_data1.to_string(), data: item_data1.to_string().into_boxed_str().into_boxed_bytes() };
        let item2 = Item { id: item_data2.to_string(), data: item_data2.to_string().into_boxed_str().into_boxed_bytes() };

        items.push(item1.clone());
        items.push(item2.clone());

        work_queue1.add_item(db, &item1).await?;
        work_queue1.add_item(db, &item2).await?;

        if rand::thread_rng().gen_range(0..10) < 3 {
            work_queue1.lease(db, Some(Duration::from_secs(1)), Duration::from_secs(4)).await?;
        }

        if rand::thread_rng().gen_range(0..10) < 5 {
            if let Some(item) = items.pop() {
                work_queue1.complete(db, &item).await?;
            }
        }
    }

    let main_queue_key = key_prefix.of("queue");
    
    let processing_key = key_prefix.of("processing");
    let main_items: Vec<String> = db.lrange(&main_queue_key, 0, -1).await?;
    let processing_items: Vec<String> = db.lrange(&processing_key, 0, -1).await?;
    for item in &main_items {
        if processing_items.contains(item) {
            return Err(redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Found duplicated item from processing queue inside main queue",
            )));
        }
    }

    Ok(())
}


async fn async_main() -> RedisResult<()> {
    let host = std::env::args()
        .skip(1)
        .next()
        .expect("first command line argument must be redis host");
    let db = &mut redis::Client::open(format!("redis://{host}/"))?
        .get_async_connection()
        .await?;


        if let Err(err) = run_duplicate_items_test(&mut db).await {
            println!("Error: {}", err);
            return;
        }

    let rust_results_key = KeyPrefix::new("results:rust:".to_string());
    let shared_results_key = KeyPrefix::new("results:shared:".to_string());

    let rust_queue = WorkQueue::new(KeyPrefix::new("rust_jobs".to_string()));
    let shared_queue = WorkQueue::new(KeyPrefix::new("shared_jobs".to_string()));

    let mut rust_job_counter = 0;
    let mut shared_job_counter = 0;

    let mut shared = false;
    loop {
        shared = !shared;
        if shared {
            shared_job_counter += 1;

            // First, try to get a job from the shared job queue
			let timeout = if shared_job_counter%5 == 0 {
                Some(Duration::from_secs(1))
            } else {
                Some(Duration::ZERO)
            };
            println!("Leasing from shared with timeout: {:?}", timeout);
            let Some(job) = shared_queue.lease(
                db,
                timeout,
                Duration::from_secs(2),
            ).await? else { continue };
            // Also, if we get 'unlucky', crash while completing the job.
            if shared_job_counter % 7 == 0 {
                println!("Dropping job");
                continue;
            }

            // Parse the data
            let data: SharedJobData = job.data_json().unwrap();
            // Generate the response
            let result = SharedJobResult {
                a: data.a,
                sum: data.a + data.b,
                prod: data.a * data.b,
                worker: "rust".to_string(),
            };
            let result_json = serde_json::to_string(&result).unwrap();
            println!("Result: {result_json}");
            // Pretend it takes us a while to compute the result
            // Sometimes this will take too long and we'll timeout
            if shared_job_counter % 12 == 0 {
                std::thread::sleep(Duration::from_secs(shared_job_counter % 4));
            }

            // Store the result
            db.set(shared_results_key.of(&job.id), result_json).await?;

            // Complete the job unless we're 'unlucky' and crash again
            if shared_job_counter % 29 != 0 {
                println!("Completing");
                shared_queue.complete(db, &job).await?;
            } else {
                println!("Dropping");
            }
        } else {
            rust_job_counter += 1;

            // First, try to get a job from the rust job queue
			let timeout = if shared_job_counter%6 == 0 {
                Some(Duration::from_secs(2))
            } else {
                Some(Duration::ZERO)
            };
            println!("Leasing from rust with timeout: {:?}", timeout);
            let Some(job) = rust_queue.lease(
                db,
                timeout,
                Duration::from_secs(1),
            ).await? else { continue };
            // Also, if we get 'unlucky', crash while completing the job.
            if rust_job_counter % 7 == 0 {
                println!("Dropping job");
                continue;
            }

            // Check the data is a single byte
            assert_eq!(job.data.len(), 1);
            // Generate the response
            let result: u8 = job.data[0].wrapping_mul(7);
            println!("Result: {result}");
            // Pretend it takes us a while to compute the result
            // Sometimes this will take too long and we'll timeout
            if rust_job_counter % 25 == 0 {
                std::thread::sleep(Duration::from_secs(rust_job_counter % 20));
            }

            // Store the result
            db.set(rust_results_key.of(&job.id), vec![result]).await?;

            // Complete the job unless we're 'unlucky' and crash again
            if rust_job_counter % 29 != 0 {
                println!("Completing");
                if rust_queue.complete(db, &job).await? {
                    println!("Spawning shared jobs");
                    // If we succesfully completed the result, create two new shared jobs.
                    let item = Item::from_json_data(&SharedJobData {
                        a: 3,
                        b: job.data[0] as i32,
                    })
                    .unwrap();
                    shared_queue.add_item(db, &item).await?;

                    let item = Item::from_json_data(&SharedJobData {
                        a: 5,
                        b: job.data[0] as i32,
                    })
                    .unwrap();
                    shared_queue.add_item(db, &item).await?;
                }
            } else {
                println!("Dropping");
            }
        }
    }
}
