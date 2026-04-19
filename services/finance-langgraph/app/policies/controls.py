from typing import List, Dict, Any


MATERIALITY_THRESHOLD = 10000.0
CONFIDENCE_THRESHOLD = 0.80
FX_VARIANCE_PERCENT_THRESHOLD = 5.0


def evaluate_controls(explanations: List[Dict[str, Any]]) -> Dict[str, Any]:
    approvals_required = False
    materiality_breach = False
    controls_passed = True
    reasons = []
    approval_items = []

    for item in explanations:
        variance = float(item.get("variance_amount", 0) or 0)
        confidence = float(item.get("confidence_score", 0) or 0)
        exception_type = item.get("exception_type", "UNKNOWN")
        explanation = item.get("explanation", "")
        exception_id = item.get("exception_id")

        item_reasons = []

        if variance > MATERIALITY_THRESHOLD:
            approvals_required = True
            materiality_breach = True
            item_reasons.append(f"Variance {variance} exceeds materiality threshold {MATERIALITY_THRESHOLD}")

        if confidence < CONFIDENCE_THRESHOLD:
            approvals_required = True
            item_reasons.append(f"Confidence {confidence} below threshold {CONFIDENCE_THRESHOLD}")

        if "FX" in explanation.upper() and variance > 0:
            fx_percent = 100.0
            if fx_percent > FX_VARIANCE_PERCENT_THRESHOLD:
                approvals_required = True
                item_reasons.append(
                    f"FX-related variance exceeds threshold {FX_VARIANCE_PERCENT_THRESHOLD}%"
                )

        if item_reasons:
            approval_items.append({
                "exception_id": exception_id,
                "exception_type": exception_type,
                "variance_amount": variance,
                "confidence_score": confidence,
                "reasons": item_reasons,
            })
            reasons.extend(item_reasons)

    return {
        "controls_passed": controls_passed,
        "approvals_required": approvals_required,
        "materiality_breach": materiality_breach,
        "reasons": reasons,
        "approval_items": approval_items,
    }
