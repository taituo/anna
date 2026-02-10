use std::collections::HashMap;

pub fn subst(
    input: &str,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> String {
    let mut all: HashMap<String, String> = HashMap::with_capacity(vars.len() + outputs.len());
    let mut keys = Vec::with_capacity(vars.len() + outputs.len());

    for (k, v) in vars {
        all.insert(k.clone(), v.clone());
        keys.push(k.clone());
    }
    for (k, v) in outputs {
        all.insert(k.clone(), v.clone());
        keys.push(k.clone());
    }

    keys.sort_by(|a, b| b.len().cmp(&a.len()));
    keys.dedup();

    let mut out = input.to_string();
    for key in keys {
        if let Some(value) = all.get(&key) {
            out = out.replace(&format!("${}", key), value);
        }
    }
    out
}

pub fn eval_when(
    cond: Option<&str>,
    outputs: &HashMap<String, String>,
    success: &HashMap<String, bool>,
) -> bool {
    let Some(raw) = cond else {
        return true;
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return true;
    }

    if let Some(parts) = split_outside_quotes(raw, "||") {
        return parts.iter().any(|p| eval_when(Some(p), outputs, success));
    }
    if let Some(parts) = split_outside_quotes(raw, "&&") {
        return parts.iter().all(|p| eval_when(Some(p), outputs, success));
    }

    let substituted = substitute_condition_values(raw, outputs, success);
    eval_atomic(&substituted)
}

fn substitute_condition_values(
    cond: &str,
    outputs: &HashMap<String, String>,
    success: &HashMap<String, bool>,
) -> String {
    let mut out = cond.to_string();

    let mut success_keys: Vec<&String> = success.keys().collect();
    success_keys.sort_by(|a, b| b.len().cmp(&a.len()));
    for key in success_keys {
        let marker = format!("${}.success", key);
        let value = success.get(key).copied().unwrap_or(false).to_string();
        out = out.replace(&marker, &value);
    }

    let mut output_keys: Vec<&String> = outputs.keys().collect();
    output_keys.sort_by(|a, b| b.len().cmp(&a.len()));
    for key in output_keys {
        let marker = format!("${}", key);
        if let Some(value) = outputs.get(key) {
            out = out.replace(&marker, value);
        }
    }

    clear_unresolved_markers(&out)
}

fn eval_atomic(expr: &str) -> bool {
    let expr = expr.trim();

    if let Some((left, right)) = split_once(expr, " contains ") {
        return normalize_value(left).contains(&normalize_value(right));
    }
    if let Some((left, right)) = split_once(expr, "!=") {
        return normalize_value(left) != normalize_value(right);
    }
    if let Some((left, right)) = split_once(expr, "==") {
        return normalize_value(left) == normalize_value(right);
    }

    match normalize_value(expr).as_str() {
        "true" => true,
        "false" => false,
        other => !other.is_empty(),
    }
}

fn split_once<'a>(input: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    input.find(op).map(|idx| {
        let left = &input[..idx];
        let right = &input[idx + op.len()..];
        (left.trim(), right.trim())
    })
}

fn normalize_value(v: &str) -> String {
    let mut s = v.trim().to_string();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s = s[1..s.len() - 1].to_string();
    }
    s
}

fn clear_unresolved_markers(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '$' {
            i += 1;
            while i < chars.len() {
                let c = chars[i];
                if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
                    i += 1;
                } else {
                    break;
                }
            }
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

pub fn split_outside_quotes(input: &str, op: &str) -> Option<Vec<String>> {
    let op_bytes = op.as_bytes();
    if op_bytes.is_empty() {
        return None;
    }

    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut start = 0usize;
    let mut in_single_quote = false;
    let mut parts = Vec::new();

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_single_quote = !in_single_quote;
            i += 1;
            continue;
        }

        if !in_single_quote
            && i + op_bytes.len() <= bytes.len()
            && &bytes[i..i + op_bytes.len()] == op_bytes
        {
            let part = input[start..i].trim().to_string();
            parts.push(part);
            i += op_bytes.len();
            start = i;
            continue;
        }

        i += 1;
    }

    if parts.is_empty() {
        return None;
    }
    parts.push(input[start..].trim().to_string());
    Some(parts)
}

#[cfg(test)]
mod tests {
    use super::{eval_when, split_outside_quotes, subst};
    use std::collections::HashMap;

    #[test]
    fn subst_uses_longest_key_first() {
        let mut vars = HashMap::new();
        vars.insert("DC".to_string(), "x".to_string());
        vars.insert("DC_NAME".to_string(), "prod".to_string());
        let outputs = HashMap::new();
        let out = subst("env=$DC_NAME.$DC", &vars, &outputs);
        assert_eq!(out, "env=prod.x");
    }

    #[test]
    fn eval_when_handles_and_or_contains() {
        let mut outputs = HashMap::new();
        outputs.insert("check".to_string(), "PASS".to_string());
        outputs.insert("a".to_string(), "ok".to_string());
        let mut success = HashMap::new();
        success.insert("build".to_string(), true);

        assert!(eval_when(
            Some("$check contains 'PASS'"),
            &outputs,
            &success
        ));
        assert!(eval_when(
            Some("$a == 'ok' && $build.success == true"),
            &outputs,
            &success
        ));
        assert!(eval_when(
            Some("$missing == '' || $check == 'PASS'"),
            &outputs,
            &success
        ));
    }

    #[test]
    fn split_respects_quotes() {
        let parts = split_outside_quotes("a == 'x || y' || b == 'z'", "||")
            .expect("split should produce parts");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "a == 'x || y'");
        assert_eq!(parts[1], "b == 'z'");
    }
}
