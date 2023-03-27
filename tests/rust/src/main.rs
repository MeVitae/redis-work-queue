use std::time::Duration;

use futures_lite::future;
use redis::{AsyncCommands, RedisResult};
use serde::{Deserialize, Serialize};

use redis_work_queue::{Item, KeyPrefix, WorkQueue};

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

async fn async_main() -> RedisResult<()> {
    let host = std::env::args()
        .skip(1)
        .next()
        .expect("first command line argument must be redis host");
    let db = &mut redis::Client::open(format!("redis://{host}/"))?
        .get_async_connection()
        .await?;

    let rust_results_key = KeyPrefix::new("results:rust:".to_string());
    let shared_results_key = KeyPrefix::new("results:shared:".to_string());

    let rust_queue = WorkQueue::new(KeyPrefix::new("go_jobs".to_string()));
    let shared_queue = WorkQueue::new(KeyPrefix::new("shared_jobs".to_string()));

    let mut rust_job_counter = 0;
    let mut shared_job_counter = 0;

    let mut shared = false;
    loop {
        shared = !shared;
        if shared {
            shared_job_counter += 1;

            // First, try to get a job from the shared job queue
            let Some(job) = shared_queue.lease(
                db,
                Some(Duration::from_secs(4)),
                Duration::from_secs(2),
            ).await? else { continue };
            // Also, if we get 'unlucky', crash while completing the job.
            if shared_job_counter % 7 == 0 {
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
            // Pretend it takes us a while to compute the result
            // Sometimes this will take too long and we'll timeout
            std::thread::sleep(Duration::from_secs(shared_job_counter % 7));

            // Store the result
            db.set(shared_results_key.of(&job.id), result_json).await?;

            // Complete the job unless we're 'unlucky' and crash again
            if shared_job_counter % 29 != 0 {
                shared_queue.complete(db, &job).await?;
            }
        } else {
            rust_job_counter += 1;

            // First, try to get a job from the rust job queue
            let Some(job) = rust_queue.lease(
                db,
                Some(Duration::from_secs(2)),
                Duration::from_secs(1),
            ).await? else { continue };
            // Also, if we get 'unlucky', crash while completing the job.
            if rust_job_counter % 7 == 0 {
                continue;
            }

            // Check the data is a single byte
            assert_eq!(job.data.len(), 1);
            // Generate the response
            let result: u8 = job.data[0].wrapping_mul(7);
            // Pretend it takes us a while to compute the result
            // Sometimes this will take too long and we'll timeout
            if rust_job_counter % 11 == 0 {
                std::thread::sleep(Duration::from_secs(rust_job_counter % 20));
            }

            // Store the result
            db.set(rust_results_key.of(&job.id), vec![result]).await?;

            // Complete the job unless we're 'unlucky' and crash again
            if rust_job_counter % 29 != 0 {
                if rust_queue.complete(db, &job).await? {
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
            }
        }
    }
}
