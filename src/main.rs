use executor::ExecutorRef;
use executor::Filter;
use executor::HashJoin;
use executor::PipelineExecutor;
use executor::TableScan;
use releation::Student;
use releation::Table;
use std::sync::Arc;

use crate::executor::Limit;

mod executor;
mod releation;

fn main() {
    let mut student: Vec<Student> = Vec::new();
    (0..1024 * 4)
        .into_iter()
        .for_each(|x| student.push(Student { id: x }));
    let build_table = Table { row_sets: student };

    let mut student: Vec<Student> = Vec::new();
    (1024 * 2..1024 * 4)
        .into_iter()
        .for_each(|x| student.push(Student { id: x }));
    let probe_table = Table { row_sets: student };

    let table_scan_a = Arc::new(TableScan::new(build_table));
    let table_scan_b = Arc::new(TableScan::new(probe_table));
    let filter = Arc::new(Filter::new(table_scan_a.clone()));
    let hash_join = Arc::new(HashJoin::new(filter.clone(), table_scan_b.clone()));

    // streaming
    let limit = Arc::new(Limit::new(hash_join.clone(), 100, 200));

    let mut executors: Vec<ExecutorRef> = Vec::with_capacity(3);
    executors.push(table_scan_a);
    executors.push(filter);
    executors.push(hash_join.clone());
    let mut pipeline_executor = PipelineExecutor::new(executors, false);

    let mut executors: Vec<ExecutorRef> = Vec::with_capacity(3);
    executors.push(table_scan_b);
    executors.push(hash_join);
    executors.push(limit); // pull

    let mut pull_executor = PipelineExecutor::new(executors, true);
    pipeline_executor.execute();
    pull_executor.execute();
    let result_set = pull_executor.get_result();
    println!("result set: len {}", result_set.rows_num());
    println!("result set: columns len {}", result_set.columns_num());
    assert_eq!(result_set.rows_num(), 200);
    assert_eq!(result_set.columns_num(), 2);
    // first value should be 2660, chunck's len should be 200 
    for idx in 0..200 {
        let id = result_set.get_value(idx, 0);
        let age = result_set.get_value(idx, 1);
        println!("join result: id = {}, age = {}", id, age);
    }
}
/*                               Limit         |<without push> <early break>
 *                                 |           | 
 * (push the data to ht)        HashJoin       |
 * |<build ht>                 |         |     |<probe ht>
 * |                         Filter     Scan   |
 * |                           |               (piepeline 2)
 * |                          Scan             (get data from source operator)
 * (pipeline 1)
 * 
 *
 */

/*
 * 1、Get Data From Source
 * 2、Apply Op To Data
 * 3、<Optional> Push Data To ...
 *
 * Pipleline 2 should depend on pipeline 1
 *
 * Pull Based + Push Based
 *
 * Advantage:
 * 1、Data flow control is extracted to the pipeline executor
 * 2、Ability to schedule pipelines tasks in parallel
*/
