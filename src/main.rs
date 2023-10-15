//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/// refs:
/// - https://en.wikipedia.org/wiki/Pipeline_(software)
/// - https://go.dev/blog/pipelines: highlights an essential challenge - stages not exiting when they should, resulting in resource leak.
/// A pipeline is a series of stages connected by channels
/// In each stage:
///     - receive values from upstream via inbound channels
///     - perform some function on that data, usually producing new values
///     - send values downstream via outbound channels
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/// The use of channels for communication between stages means that stages can also be run in parallel.
/// This use case here is the following steps:
///     - generate numbers
///     - square them, using several workers
///     - merge the results from the various workers
use std::collections::VecDeque;
use std::sync::mpsc::{channel, Sender};
use std::thread;

enum PipelineMsg {
    Generated(u8),
    Squared(u8),
    Merged(u8),
}

fn generate(num_tx: Sender<PipelineMsg>) {
    let mut num = 2;
    let _ = thread::Builder::new().spawn(move || {
        while let Ok(_) = num_tx.send(PipelineMsg::Generated(num)) {
            println!("generated {:?}", num);
            num = num + 1;
        }
        println!("num_tx dropped");
    });
}

fn square(merge_chan: Sender<PipelineMsg>) -> Sender<PipelineMsg> {
    let (stx, srx) = channel();
    let _ = thread::Builder::new().spawn(move || {
        for msg in srx {
            let num = match msg {
                PipelineMsg::Generated(num) => num,
                _ => panic!("Unexpected message receiving at square stage"),
            };
            let _ = merge_chan.send(PipelineMsg::Squared(num * num));
            println!("merge received {:?}", num);
        }
        println!("square sender dropped");
    });
    stx
}

fn merge(merged_result_chan: Sender<PipelineMsg>) -> Sender<PipelineMsg> {
    let (mtx, mrx) = channel();
    let _ = thread::Builder::new().spawn(move || {
        for msg in mrx {
            let squared = match msg {
                PipelineMsg::Squared(num) => num,
                _ => panic!("Unexpected message receiving at merge stage"),
            };
            println!("merge received {:?}", squared);
            let _ = merged_result_chan.send(PipelineMsg::Merged(squared));
        }
        println!("merged sender dropped");
    });
    mtx
}

fn main() {
    // Create a channel for the results.
    let (results_tx, results_rx) = channel();
    // Create a channel for the generated numbers.
    let (generated_tx, generated_rx) = channel();
    // Create a channel for the merged results.
    let merge_tx = merge(results_tx);
    // from here, we introduce an extra scope, which will result in the queue of the “worker sender” to drop
    // create new scope to drop the square workers!
    {
        let mut square_workers: VecDeque<Sender<PipelineMsg>> =
            vec![square(merge_tx.clone()), square(merge_tx)]
                .into_iter()
                .collect();
        generate(generated_tx);
        // When we drop the generated_rx, generate() will quit.
        // Receive generated numbers from the "generate" stage.
        for msg in generated_rx {
            let generated_num = match msg {
                PipelineMsg::Generated(num) => num,
                _ => panic!("Unexpected message receiving from generated stage"),
            };
            // Cycle through the workers and distribute work.
            let worker = square_workers.pop_front().unwrap();
            let _ = worker.send(msg);
            square_workers.push_back(worker);
            if generated_num == 3 {
                // breaking out of the loop, resulting in a few drops.
                // Dropping the generated_tx, stopping the generator.
                break;
            }
        }
        // At this point, gen_port will drop,
        // meaning "generate" will stop looping and sending.
        // Also, square_workers will drop,
        // meaning the workers will stop receiving,
        // and drop their clone of the merge_tx.
        // When they drop all merge_tx, "merge" will stop receiving,
        // and drop our results_tx.
    }

    // At this point, we're emptying the results channel,
    // the corresponding sender, the "results_tx" held by merge, has been dropped already,
    // so the iteration will stop once all messages have been received.
    for result in results_rx {
        // Receive "merged results" from the "merge" stage.
        match result {
            PipelineMsg::Merged(_) => continue,
            _ => panic!("Unexpected result"),
        }
    }
}
