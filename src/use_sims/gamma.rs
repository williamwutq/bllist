use rand::Rng;

fn next_f64<R: Rng>(rng: &mut R) -> f64 {
    (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
}

/// Samples from Gamma(shape=k, scale=1) using the Marsaglia-Tsang method.
/// Returns a non-negative f64.
pub fn sample_gamma_standard<R: Rng>(rng: &mut R, k: f64) -> f64 {
    assert!(k > 0.0, "shape k must be positive");

    if k < 1.0 {
        // Boost trick: Gamma(k) = Gamma(k+1) * U^(1/k)
        let u = next_f64(rng);
        return sample_gamma_standard(rng, k + 1.0) * u.powf(1.0 / k);
    }

    // Marsaglia-Tsang algorithm (works for k >= 1)
    let d = k - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();

    loop {
        // Draw a standard normal via Box-Muller
        let u1 = next_f64(rng);
        let u2 = next_f64(rng);
        let x = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();

        let v_raw = 1.0 + c * x;
        if v_raw <= 0.0 {
            continue;
        }
        let v = v_raw * v_raw * v_raw; // v = (1 + cx)^3

        let u = next_f64(rng);

        // Squeeze check (fast path)
        if u < 1.0 - 0.0331 * (x * x) * (x * x) {
            return d * v;
        }
        // Log check (slow path, rarely reached)
        if u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
            return d * v;
        }
    }
}

/// Generates a u64 in [0, max] following a Gamma(k, theta) distribution.
///
/// # Arguments
/// * `rng`   - any [`Rng`] source
/// * `max`   - upper bound of the output range
/// * `k`     - shape parameter (k > 1 gives the 0-at-0, peak, decay shape)
/// * `theta` - scale parameter (stretches the distribution)
pub fn gamma_sample_u64<R: Rng>(rng: &mut R, max: u64, k: f64, theta: f64) -> u64 {
    assert!(k > 0.0, "shape must be positive");
    assert!(theta > 0.0, "scale must be positive");

    loop {
        // Sample from Gamma(k, theta)
        let sample = sample_gamma_standard(rng, k) * theta;

        // The gamma distribution has no hard upper bound, so we rejection-sample
        // values that fall outside [0, max]. For well-chosen k/theta this is rare.
        // Suggested heuristic: set theta so that the mode (k-1)*theta is near max/2.
        let scaled = sample.round() as i64;
        if scaled >= 0 && scaled <= max as i64 {
            return scaled as u64;
        }
    }
}
