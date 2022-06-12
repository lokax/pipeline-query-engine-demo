use crate::releation::Table;
use std::{cell::RefCell, collections::HashMap, sync::Arc};


pub struct Column(Vec<u32>);

type ColumnRef = Arc<Column>;

pub type ExecutorRef = Arc<dyn PushFashion>;

#[derive(Clone)]
pub struct DataChunk {
    columns: Vec<ColumnRef>,
}

impl DataChunk {
    fn new() -> Self {
        DataChunk {
            columns: Vec::new(),
        }
    }

    fn push_columns(&mut self, column: ColumnRef) {
        self.columns.push(column);
    }

    pub fn columns_num(&self) -> usize {
        self.columns.len()
    }

    pub fn rows_num(&self) -> usize {
        self.columns[0].0.len()
    }

    pub fn get_value(&self, row: usize, col: usize) -> u32 {
        self.columns[col].0[row]
    }

    fn reset(&mut self) {
        self.columns = Vec::new();
    }
}

pub trait PushFashion {
    fn get_data(&self, _result: &mut DataChunk) {
        unimplemented!("");
    }

    fn execute(&self, _input: &DataChunk) -> DataChunk {
        unimplemented!("");
    }

    fn push(&self, _input: &DataChunk) {
        unimplemented!("");
    }

    //TODO(lokax): use state
    fn is_end(&self) -> bool {
        false
    }
}

pub struct TableScan {
    table: Table,
    group_idx: RefCell<usize>, // just for easy
                               // TODO(lokax): Create a state, Pass in PushFashion::Execute, Avoid RefCell
}

impl TableScan {
    pub fn new(table: Table) -> Self {
        Self {
            table,
            group_idx: RefCell::new(0),
        }
    }
}

impl PushFashion for TableScan {
    fn get_data(&self, result: &mut DataChunk) {
        let datas = self.table.get_rows_group(*self.group_idx.borrow());
        println!(
            "datas num {}, idx {}",
            datas.len(),
            *self.group_idx.borrow()
        );
        *self.group_idx.borrow_mut() += 1;
        let mut id_column = Column(Vec::new());
        for student in datas {
            id_column.0.push(student.get_id());
        }
        result.push_columns(Arc::new(id_column));
    }
}

pub struct Filter {
    child: ExecutorRef, // just for build, for now, it is not important
}

impl Filter {
    pub fn new(child: ExecutorRef) -> Self {
        Self { child }
    }
}

impl PushFashion for Filter {
    fn execute(&self, input: &DataChunk) -> DataChunk {
        let mut idx = 0;
        let size = input.rows_num();
        let mut column = Column(Vec::new());
        while idx < size {
            let value = input.get_value(idx, 0);
            if value >= (1024 * 2 + 512) {
                column.0.push(value);
            }
            idx += 1;
        }
        let mut result = DataChunk::new();
        result.push_columns(Arc::new(column));
        result
    }
}

pub struct HashJoin {
    left: ExecutorRef,
    right: ExecutorRef,
    ht: RefCell<HashMap<u32, u32>>, // joins key --> tuple(just one column for easy)
}

impl HashJoin {
    pub fn new(left: ExecutorRef, right: ExecutorRef) -> Self {
        Self {
            left,
            right,
            ht: RefCell::default(),
        }
    }
}

impl PushFashion for HashJoin {
    fn execute(&self, input: &DataChunk) -> DataChunk {
        let mut idx = 0;
        let size = input.rows_num();
        let mut column_left = Column(Vec::new()); // build table columns
        let mut column_right = Column(Vec::new()); // probe table columns
        while idx < size {
            let key = input.get_value(idx, 0); // probe table value
            let ht = self.ht.borrow();
            let value = ht.get(&key);
            if value.is_some() {
                column_right.0.push(key);
                column_left.0.push(value.unwrap().clone());
            }
            idx += 1;
        }
        let mut result = DataChunk::new();
        result.push_columns(Arc::new(column_left));
        result.push_columns(Arc::new(column_right));
        result
    }

    // push data to hash table(build side)
    fn push(&self, input: &DataChunk) {
        let mut idx = 0;
        let size = input.rows_num();
        while idx < size {
            let value = input.get_value(idx, 0);
            self.ht.borrow_mut().insert(value, value);
            idx += 1;
        }
    }
}

pub struct Limit {
    child: ExecutorRef,
    offset: RefCell<usize>,
    limit: RefCell<usize>,
    cursor: RefCell<usize>,
}

impl Limit {
    pub fn new(child: ExecutorRef, offset: usize, limit: usize) -> Self {
        Self {
            child,
            offset: RefCell::new(offset),
            limit: RefCell::new(limit),
            cursor: RefCell::new(0),
        }
    }
}

// Limit 5, 10;
impl PushFashion for Limit {
    fn execute(&self, input: &DataChunk) -> DataChunk {
        let max_data_count = *self.offset.borrow() + *self.limit.borrow();
        if *self.cursor.borrow() >= max_data_count {
            // TODO(lokax): Return a finished state to exist execute_pull early
            return DataChunk::new();
        }

        let current_offset = *self.cursor.borrow();
        let offset = *self.offset.borrow();
        let limit = *self.limit.borrow();

        // just hard code for test
        let mut column_a = Column(Vec::new()); // TODO(lokax): use vector
        let mut column_b = Column(Vec::new());

        let mut chunk = DataChunk::new();
        if current_offset < offset {
            if input.rows_num() + *self.cursor.borrow() < *self.offset.borrow() {
                *self.cursor.borrow_mut() += input.rows_num();
                return chunk;
            } else {
                let start_offset = offset - current_offset;
                let data_count = usize::min(input.rows_num() - start_offset, limit);
                for idx in start_offset..start_offset + data_count {
                    column_a.0.push(input.get_value(idx, 0));
                    column_b.0.push(input.get_value(idx, 1));
                }
                chunk.push_columns(Arc::new(column_a));
                chunk.push_columns(Arc::new(column_b));
                *self.cursor.borrow_mut() += input.rows_num();
                return chunk;
            }
        } else {
            let chunk_count;
            if current_offset + input.rows_num() >= limit {
                chunk_count = limit - current_offset;
            } else {
                chunk_count = input.rows_num();
            }

            for idx in current_offset..current_offset + chunk_count {
                column_a.0.push(input.get_value(idx, 0));
                column_b.0.push(input.get_value(idx, 1));
            }
            chunk.push_columns(Arc::new(column_a));
            chunk.push_columns(Arc::new(column_b));
            *self.cursor.borrow_mut() += input.rows_num();
            return chunk;
        }
    }

    fn is_end(&self) -> bool {
        *self.cursor.borrow_mut() >= *self.offset.borrow() + *self.limit.borrow()
    }
}

// push
pub struct PipelineExecutor {
    executors: Vec<ExecutorRef>,
    chunks: Vec<DataChunk>,
    pull: bool,
}

impl PipelineExecutor {
    pub fn new(executors: Vec<ExecutorRef>, pull: bool) -> Self {
        let mut chunks = Vec::new();
        chunks.resize(executors.len(), DataChunk::new());
        Self {
            executors,
            chunks,
            pull,
        }
    }

    pub fn execute(&mut self) {
        if self.pull {
            self.execute_pull();
        } else {
            self.execute_push();
        }
    }

    pub fn execute_push(&mut self) {
        'out: loop {
            self.executors[0].get_data(&mut self.chunks[0]);
            if self.chunks[0].rows_num() == 0 {
                break;
            }

            let mut idx = 1;
            let len = self.executors.len() - 1;
            let mut finished = true;
            while idx < len {
                // TODO(lokax): Resolve Borrow Problem
                let chunk = self.executors[idx].execute(&self.chunks[idx - 1]);

                if self.executors[idx].is_end() {
                    break 'out;
                }

                if chunk.rows_num() == 0 {
                    finished = false;
                    break;
                }
                self.chunks[idx] = chunk;
                idx += 1;
            }

            if finished {
                self.executors[len].push(&self.chunks[len - 1]);
            }
            self.reset_chunck();
        }
    }

    // Without Push Phase
    pub fn execute_pull(&mut self) {
        'out: loop {
            self.executors[0].get_data(&mut self.chunks[0]);
            if self.chunks[0].rows_num() == 0 {
                break;
            }
            let mut idx = 1;
            let len = self.executors.len();
            while idx < len {
                // TODO(lokax): Resolve Borrow Problem
                let chunk = self.executors[idx].execute(&self.chunks[idx - 1]);
                self.chunks[idx] = chunk;
                if self.executors[idx].is_end() {
                    println!("out");
                    break 'out;
                }
                if self.chunks[idx].rows_num() == 0 {
                    break;
                }
                idx += 1;
            }
            self.reset_chunck();
        }
    }

    pub fn get_result(&self) -> &DataChunk {
        self.chunks.last().unwrap()
    }

    fn reset_chunck(&mut self) {
        self.chunks[0].reset();
    }
}
