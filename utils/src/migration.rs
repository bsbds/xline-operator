use std::cmp::Ordering;

use anyhow::Result;

/// Compare two CRD version
/// # Errors
/// If the version dose not match the `v<number>`
#[inline]
pub fn compare_versions(version1: &str, version2: &str) -> Result<Ordering> {
    let v1_parts: u32 = version1.trim_start_matches('v').parse()?;
    let v2_parts: u32 = version2.trim_start_matches('v').parse()?;
    if v1_parts > v2_parts {
        return Ok(Ordering::Greater);
    }
    if v2_parts > v1_parts {
        return Ok(Ordering::Less);
    }
    Ok(Ordering::Equal)
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use super::compare_versions;

    #[test]
    fn test_cmp_version() {
        assert_eq!(Ordering::Greater, compare_versions("v2", "v1").unwrap());
        assert_eq!(Ordering::Less, compare_versions("v1", "v2").unwrap());
        assert_eq!(Ordering::Equal, compare_versions("v1", "v1").unwrap());
    }
}