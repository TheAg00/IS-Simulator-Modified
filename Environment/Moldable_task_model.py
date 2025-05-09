# Υπολογισμός της νέας διάρκειας μιας εργασίας,
# με βάση ένα συγκεκριμένο αριθμό πυρήνων n, σύμφωνα με ένα low-variance μοντέλο.
def duration_with_nLOW(job, alpha, n, sigma):
    initialSpeedup = speedupLOW(n = job.req, alpha = alpha, sigma = sigma)
    newSpeedup = speedupLOW(n = n, alpha = alpha, sigma = sigma)
    speedupRatio = initialSpeedup / newSpeedup
    return job.dur * speedupRatio


# Υπολογισμός της νέας διάρκειας μιας εργασίας,
# με βάση ένα συγκεκριμένο αριθμό πυρήνων n, σύμφωνα με ένα high-variance μοντέλο.
def duration_with_nHIGH(job, alpha, n, sigma):
    initialSpeedup = speedupHIGH(n = job.req, alpha = alpha, sigma = sigma)
    newSpeedup = speedupHIGH(n = n, alpha = alpha, sigma = sigma)
    speedupRatio = initialSpeedup / newSpeedup
    return job.dur * speedupRatio


# Υπολογισμός του speedup μιας εργασίας, σύμφωνα με ένα low-variance μοντέλο.
# Το speedup  μετρά πόσο γρηγορότερα εκτελείται μια εργασία όταν τρέχει σε πολλαπλούς πυρίνες,
# σε σύγκριση με την εκτέλεσή του σε έναν μόνο πυρίνα.
def speedupLOW(n, alpha, sigma):
    if 1 <= n <= alpha: return (alpha * n) / (alpha + (sigma / 2) * (n - 1))

    if alpha <= n <= 2 * alpha - 1: return (alpha * n) / (sigma * (alpha - 0.5) + n * (1 - sigma / 2))

    return alpha


# Υπολογισμός του speedup μιας εργασίας, σύμφωνα με ένα high-variance μοντέλο.
def speedupHIGH(n, alpha, sigma):
    if 1 <= n <= (alpha + (alpha * sigma) - sigma): return ((n * alpha) * (sigma + 1)) / (sigma * (n + alpha - 1) + alpha)

    return alpha


# Υπολογισμός του βέλτιστου αριθμού πυρήνων για μια εργασία, σύμφωνα με ένα low-variance μοντέλο.
def optimal_coresLOW(alpha, sigma, maxCores):
    nStar = (sigma * (alpha - 0.5)) / (1 - sigma / 2)
    return min(nStar, maxCores)


# Υπολογισμός του βέλτιστου αριθμού πυρήνων για μια εργασία, σύμφωνα με ένα high-variance μοντέλο.
def optimal_coresHIGH(alpha, sigma, maxCores):
    nStar = ((alpha * (sigma + 1)) - sigma) / sigma
    return min(nStar, maxCores)