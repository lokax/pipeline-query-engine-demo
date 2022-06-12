use crate::releation::Table;
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};

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

pub trait PushFashion: Send + Sync + 'static {
    fn get_data(&self, _result: &mut DataChunk, _state: &mut Box<dyn SourceState>) {
        unimplemented!();
    }

    fn execute(&self, _input: &DataChunk, _state: &mut Box<dyn ExecuteState>) -> DataChunk {
        unimplemented!();
    }

    fn materialize(&self, _input: &DataChunk) {
        unimplemented!();
    }

    //TODO(lokax): use state
    fn is_end(&self, _: &mut Box<dyn ExecuteState>) -> bool {
        false
    }

    fn get_source_state(&self) -> Box<dyn SourceState> {
        Box::new(DummyState)
    }

    fn get_execute_state(&self) -> Box<dyn ExecuteState> {
        Box::new(DummyState)
    }

    fn get_materialized_state(&self) -> Box<dyn MaterializedState> {
        Box::new(DummyState)
    }

    //TODO(lokax): Remove
    fn is_pipeline_breaker(&self) -> bool {
        false
    }
}

// push
pub struct PipelineExecutor {
    executors: Vec<ExecutorRef>,
    chunks: Vec<DataChunk>,
    states: Vec<ExecutionState>,
    _source_m_state: HashMap<usize, Box<dyn MaterializedState>>,
    pull: bool,
}

impl PipelineExecutor {
    pub fn new(executors: Vec<ExecutorRef>, pull: bool) -> Self {
        let mut chunks = Vec::new();
        chunks.resize(executors.len(), DataChunk::new());
        let mut states = vec![ExecutionState::Source(executors[0].get_source_state())];
        // TODO(lokax): Maybe Bug
        executors
            .iter()
            .take(executors.len() - 1)
            .skip(1)
            .for_each(|e| {
                states.push(ExecutionState::Operator(e.get_execute_state()));
            });
        // TODO(lokax): Remove unused code
        let _source_m_state = executors
            .iter()
            .enumerate()
            .map(|(idx, e)| {
                if e.is_pipeline_breaker() {
                    (idx, e.get_materialized_state())
                } else {
                    (idx, Box::new(DummyState) as Box<dyn MaterializedState>)
                }
            })
            .collect::<HashMap<_, _>>();

        if !pull {
            states.push(ExecutionState::Materialized(
                executors.last().unwrap().get_materialized_state(),
            ));
        } else {
            states.push(ExecutionState::Operator(
                executors.last().unwrap().get_execute_state(),
            ));
        }

        Self {
            executors,
            chunks,
            states,
            pull,
            _source_m_state,
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
            self.executors[0].get_data(&mut self.chunks[0], self.states[0].get_source_mut());
            if self.chunks[0].rows_num() == 0 {
                break;
            }

            let mut idx = 1;
            let len = self.executors.len() - 1;
            let mut finished = true;
            while idx < len {
                // TODO(lokax): Resolve Borrow Problem
                let state = self.states[idx].get_execute_mut();
                let chunk = self.executors[idx].execute(&self.chunks[idx - 1], state);

                if self.executors[idx].is_end(self.states[idx].get_execute_mut()) {
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
                self.executors[len].materialize(&self.chunks[len - 1]);
            }
            self.reset_chunck();
        }
    }

    // Without Push Phase
    pub fn execute_pull(&mut self) {
        'out: loop {
            self.executors[0].get_data(&mut self.chunks[0], self.states[0].get_source_mut());
            if self.chunks[0].rows_num() == 0 {
                break;
            }
            let mut idx = 1;
            let len = self.executors.len();
            while idx < len {
                // TODO(lokax): Resolve Borrow Problem
                let chunk = self.executors[idx]
                    .execute(&self.chunks[idx - 1], self.states[idx].get_execute_mut());
                self.chunks[idx] = chunk;
                if self.executors[idx].is_end(self.states[idx].get_execute_mut()) {
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

    pub fn get_result(mut self) -> DataChunk {
        self.chunks.pop().unwrap()
    }

    fn reset_chunck(&mut self) {
        self.chunks[0].reset();
    }
}

enum ExecutionState {
    Source(Box<dyn SourceState>),
    Operator(Box<dyn ExecuteState>),
    Materialized(Box<dyn MaterializedState>),
}

impl ExecutionState {
    fn get_source_mut(&mut self) -> &mut Box<dyn SourceState> {
        match self {
            Self::Source(state) => state,
            _ => unimplemented!(),
        }
    }

    fn get_execute_mut(&mut self) -> &mut Box<dyn ExecuteState> {
        match self {
            Self::Operator(state) => state,
            _ => unimplemented!(),
        }
    }

    // TODO(lokax): Remove unused code
    fn get_materialized_mut(&mut self) -> &mut Box<dyn MaterializedState> {
        match self {
            Self::Materialized(state) => state,
            _ => unimplemented!(),
        }
    }
}

struct DummyState;

pub struct ScanState {
    group_idx: usize,
}

pub struct TableScan {
    table: Table,
}

impl TableScan {
    pub fn new(table: Table) -> Self {
        Self { table }
    }
}

impl PushFashion for TableScan {
    fn get_data(&self, result: &mut DataChunk, state: &mut Box<dyn SourceState>) {
        let state = state.as_any_mut().downcast_mut::<ScanState>().unwrap();
        let datas = self.table.get_rows_group(state.group_idx);
        println!("datas num {}, idx {}", datas.len(), state.group_idx);
        state.group_idx += 1;
        let mut id_column = Column(Vec::new());
        for student in datas {
            id_column.0.push(student.get_id());
        }
        result.push_columns(Arc::new(id_column));
    }

    fn get_source_state(&self) -> Box<dyn SourceState> {
        Box::new(ScanState { group_idx: 0 })
    }
}

pub struct Filter {
    child: ExecutorRef, // Just for build, for now, it is not important
}

impl Filter {
    pub fn new(child: ExecutorRef) -> Self {
        Self { child }
    }
}

impl PushFashion for Filter {
    fn execute(&self, input: &DataChunk, _: &mut Box<dyn ExecuteState>) -> DataChunk {
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

    fn get_execute_state(&self) -> Box<dyn ExecuteState> {
        Box::new(DummyState)
    }
}

pub struct HashJoin {
    left: ExecutorRef,
    right: ExecutorRef,
    m_state: Mutex<JoinHtState>, // joins key --> tuple(just one column for easy)
}

impl HashJoin {
    pub fn new(left: ExecutorRef, right: ExecutorRef) -> Self {
        Self {
            left,
            right,
            // ht: RefCell::default(),
            m_state: Mutex::new(JoinHtState {
                ht: HashMap::default(),
            }),
        }
    }
}

struct JoinHtState {
    ht: HashMap<u32, u32>,
}

impl PushFashion for HashJoin {
    fn is_pipeline_breaker(&self) -> bool {
        true
    }

    fn execute(&self, input: &DataChunk, _e_state: &mut Box<dyn ExecuteState>) -> DataChunk {
        let mut idx = 0;
        let size = input.rows_num();
        let mut column_left = Column(Vec::new()); // build table columns
        let mut column_right = Column(Vec::new()); // probe table columns

        // let ht_state = m_state.as_any_mut().downcast_mut::<JoinHtState>().unwrap();
        let ht_state = self.m_state.lock().unwrap();
        while idx < size {
            let key = input.get_value(idx, 0); // probe table value
            let ht = &ht_state.ht;
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
    fn materialize(&self, input: &DataChunk) {
        let mut idx = 0;
        let size = input.rows_num();
        // let ht_state = m_state.as_any_mut().downcast_mut::<JoinHtState>().unwrap();
        let mut ht_state = self.m_state.lock().unwrap();
        let ht = &mut ht_state.ht;
        while idx < size {
            let value = input.get_value(idx, 0);
            ht.insert(value, value);
            idx += 1;
        }
    }

    fn get_materialized_state(&self) -> Box<dyn MaterializedState> {
        Box::new(JoinHtState {
            ht: HashMap::default(),
        })
    }
}

pub struct Limit {
    child: ExecutorRef,
    offset: usize,
    limit: usize,
}

impl Limit {
    pub fn new(child: ExecutorRef, offset: usize, limit: usize) -> Self {
        Self {
            child,
            offset,
            limit,
        }
    }
}

struct LimitState {
    cursor: usize,
}

// Limit 5, 10;
impl PushFashion for Limit {
    fn execute(&self, input: &DataChunk, state: &mut Box<dyn ExecuteState>) -> DataChunk {
        let limit_state = state.as_any_mut().downcast_mut::<LimitState>().unwrap();
        let max_data_count = self.offset + self.limit;
        if limit_state.cursor >= max_data_count {
            // TODO(lokax): Return a finished state to exist execute_pull early
            return DataChunk::new();
        }

        let current_offset = limit_state.cursor;
        let offset = self.offset;
        let limit = self.limit;

        // just hard code for test
        let mut column_a = Column(Vec::new()); // TODO(lokax): use vector
        let mut column_b = Column(Vec::new());

        let mut chunk = DataChunk::new();
        if current_offset < offset {
            if input.rows_num() + limit_state.cursor < self.offset {
                limit_state.cursor += input.rows_num();
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
                limit_state.cursor += input.rows_num();
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
            limit_state.cursor += input.rows_num();
            return chunk;
        }
    }

    fn get_execute_state(&self) -> Box<dyn ExecuteState> {
        Box::new(LimitState { cursor: 0 })
    }

    // TODO(lokax): Remove this.
    fn is_end(&self, state: &mut Box<dyn ExecuteState>) -> bool {
        let limit_state = state.as_any_mut().downcast_mut::<LimitState>().unwrap();
        limit_state.cursor >= self.offset + self.limit
    }
}

pub trait SourceState: Send + Sync + 'static {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait ExecuteState: Send + Sync + 'static {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait MaterializedState: Send + Sync + 'static {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// TODO(lokax): macro!
impl SourceState for DummyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!();
    }
}
impl ExecuteState for DummyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!();
    }
}

impl MaterializedState for DummyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!();
    }
}

impl SourceState for ScanState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl MaterializedState for JoinHtState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ExecuteState for LimitState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
