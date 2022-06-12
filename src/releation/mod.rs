pub struct Student {
    pub id: u32,
}

impl Student {
    pub fn get_id(&self) -> u32 {
        self.id
    }
}

pub struct Table {
    pub row_sets: Vec<Student>,
}

impl Table {
    pub fn cardinality(&self) -> usize {
        self.row_sets.len()
    }

    pub fn get_rows_group(&self, idx: usize) -> &[Student] {
        assert_eq!(self.cardinality() % 1024, 0);
        if 1024 * idx >= self.cardinality() {
            return &self.row_sets[1..1];
        }
        &self.row_sets[1024 * idx..(1024 * (idx + 1))]
    }
}
