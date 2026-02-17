    use super::{Stage, Workflow};
    use std::collections::HashMap;
    use std::time::Duration;

    fn base_workflow(stages: Vec<Stage>) -> Workflow {
        Workflow {
            name: "wf".to_string(),
            mode: "once".to_string(),
            memory: false,
            tags: vec![],
            vars: HashMap::new(),
            env: HashMap::new(),
            workdir: None,
            trigger: Default::default(),
            stages,
            source_path: None,
        }
    }

    #[test]
    fn allows_dependency_on_prior_stage() {
        let workflow = base_workflow(vec![
            Stage {
                id: "a".to_string(),
                exec: Some("echo a".to_string()),
                ..Default::default()
            },
            Stage {
                id: "b".to_string(),
                needs: vec!["a".to_string()],
                exec: Some("echo b".to_string()),
                ..Default::default()
            },
        ]);
        workflow.validate().expect("workflow should validate");
    }

    #[test]
    fn rejects_dependency_on_later_stage() {
        let invalid_workflow = base_workflow(vec![
            Stage {
                id: "b".to_string(),
                needs: vec!["a".to_string()],
                exec: Some("echo b".to_string()),
                ..Default::default()
            },
            Stage {
                id: "a".to_string(),
                exec: Some("echo a".to_string()),
                ..Default::default()
            },
        ]);
        let err = invalid_workflow
            .validate()
            .expect_err("workflow should fail validation");
        assert!(err.to_string().contains("must reference an earlier stage"));
    }

    #[test]
    fn loop_stage_does_not_implicitly_make_workflow_continuous() {
        let loop_only_workflow = base_workflow(vec![Stage {
            id: "looped".to_string(),
            loop_stage: true,
            exec: Some("echo hi".to_string()),
            ..Default::default()
        }]);
        assert!(!loop_only_workflow.is_continuous());
        assert!(loop_only_workflow.has_loop());
    }

    #[test]
    fn stage_loop_interval_uses_default_when_missing() {
        let stage = Stage {
            id: "looped".to_string(),
            loop_stage: true,
            ..Default::default()
        };
        assert_eq!(
            stage
                .loop_interval_duration()
                .expect("default loop interval should parse"),
            Duration::from_secs(1)
        );
    }
