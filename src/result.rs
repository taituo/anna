use crate::session::gen_session_id;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RunResult {
    pub session_id: String,
    pub parent_id: Option<String>,
    pub children: Vec<String>,
    pub outputs: HashMap<String, String>,
    pub success: HashMap<String, bool>,
    pub errors: Vec<String>,
}

impl RunResult {
    pub fn new() -> Self {
        let session_id = gen_session_id();
        let mut outputs = HashMap::new();
        outputs.insert("SESSION".to_string(), session_id.clone());
        Self {
            session_id,
            parent_id: None,
            children: Vec::new(),
            outputs,
            success: HashMap::new(),
            errors: Vec::new(),
        }
    }

    pub fn set_success(&mut self, stage_id: &str, output: String) {
        self.outputs.insert(stage_id.to_string(), output);
        self.success.insert(stage_id.to_string(), true);
    }

    pub fn set_error(&mut self, stage_id: &str, err: String) {
        self.success.insert(stage_id.to_string(), false);
        self.errors.push(format!("{}: {}", stage_id, err));
    }

    pub fn add_child(&mut self, session_id: String) {
        self.children.push(session_id);
    }
}
