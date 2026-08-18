"""Attribution-accuracy scoring — a pure function of (credited, truth,
exposure_household), no I/O and no clock, so it is unit-testable without
services and deterministic on the frozen fixtures.

Grain is HOUSEHOLD, not exact exposure_id (ARCHITECTURE §4.3, DECISIONS
Phase 4): the engine is last-touch, so it credits the most-recent in-window
exposure, not necessarily the causal one — scoring exact-id equality would
measure last-touch-vs-causal coincidence (a model property), not attribution
quality. Exact-id is reported only as a labeled diagnostic.

  precision = caused conversions attributed to the correct household
              / all credited (attributed=true) conversions
  recall    = caused conversions attributed to the correct household
              / all truth links

Truth links exist only for caused conversions; organic conversions have none,
so a credited organic conversion is a false positive that lowers precision.
"""

from pydantic import BaseModel


class AccuracyReport(BaseModel):
    """Household-grain accuracy plus the counts that explain it. `precision` /
    `recall` are the headline; `exact_exposure_match_rate` is the labeled
    diagnostic (expected low — see module docstring)."""

    profile: str
    credited: int
    truth_links: int
    household_correct: int
    precision: float
    recall: float
    f1: float
    exact_exposure_correct: int
    exact_exposure_match_rate: float
    organic_credited: int
    caused_missed: int
    caused_wrong_household: int


def score(
    credited: dict[str, tuple[str, str]],
    truth: dict[str, str],
    exposure_household: dict[str, str],
    profile: str = "tiny",
) -> AccuracyReport:
    """`credited`: conversion_id → (attributed household_id, credited
    exposure_id), for attributed=true rows only. `truth`: conversion_id →
    truth_exposure_id (caused conversions only). `exposure_household`:
    exposure_id → household_id (from exposures_landed)."""
    household_correct = 0
    exact_correct = 0
    caused_wrong_household = 0
    for cid, (household, exposure_id) in credited.items():
        truth_exposure = truth.get(cid)
        if truth_exposure is None:
            continue  # organic false positive — counted below via organic_credited
        if exposure_household.get(truth_exposure) == household:
            household_correct += 1
        else:
            caused_wrong_household += 1
        if exposure_id == truth_exposure:
            exact_correct += 1

    organic_credited = sum(1 for cid in credited if cid not in truth)
    caused_missed = sum(1 for cid in truth if cid not in credited)

    n_credited = len(credited)
    n_truth = len(truth)
    precision = household_correct / n_credited if n_credited else 0.0
    recall = household_correct / n_truth if n_truth else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return AccuracyReport(
        profile=profile,
        credited=n_credited,
        truth_links=n_truth,
        household_correct=household_correct,
        precision=precision,
        recall=recall,
        f1=f1,
        exact_exposure_correct=exact_correct,
        exact_exposure_match_rate=exact_correct / n_credited if n_credited else 0.0,
        organic_credited=organic_credited,
        caused_missed=caused_missed,
        caused_wrong_household=caused_wrong_household,
    )


def format_report(report: AccuracyReport) -> str:
    """A plain-text accuracy table for `make eval`. Headline = household-grain
    precision/recall; the exact-id line is explicitly labeled a diagnostic."""
    r = report
    return "\n".join(
        [
            f"attribution accuracy — profile {r.profile} (household grain)",
            "-" * 56,
            f"  credited (attributed=true) : {r.credited}",
            f"  truth links                : {r.truth_links}",
            f"  household-correct          : {r.household_correct}",
            "",
            f"  precision                  : {r.precision:.4f}"
            f"  ({r.household_correct}/{r.credited})",
            f"  recall                     : {r.recall:.4f}"
            f"  ({r.household_correct}/{r.truth_links})",
            f"  f1                         : {r.f1:.4f}",
            "",
            f"  organic over-credit (FP)   : {r.organic_credited}",
            f"  caused wrong-household     : {r.caused_wrong_household}",
            f"  caused missed              : {r.caused_missed}",
            "",
            f"  [diagnostic] exact exposure-id match : "
            f"{r.exact_exposure_match_rate:.4f} "
            f"({r.exact_exposure_correct}/{r.credited})",
            "  [diagnostic] last-touch → causal coincidence; expected low, "
            "not an accuracy measure",
        ]
    )
