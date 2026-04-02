"""
Study configs and filter presets for load testing.

Studies are chosen to cover the most demanding query patterns:
- msk_impact_50k_2026: 54k samples — largest cohort, max aggregation pressure
- msk_chord_2024:      25k samples — well-tested baseline with all data types
- ccle_broad_2019:     ~1.8k samples, 817k mutations — highest mutation count
"""
import json

EMPTY_FILTER = json.dumps({
    "clinicalDataFilters": [],
    "mutationFilter": {"genes": []},
    "svFilter": {"genes": []},
})

# Realistic active filter: age 40–70 + TP53 mutated
ACTIVE_FILTER = json.dumps({
    "clinicalDataFilters": [
        {"attributeId": "AGE", "values": [{"start": 40, "end": 70}]},
    ],
    "mutationFilter": {"genes": ["TP53"]},
    "svFilter": {"genes": []},
})

STUDIES = [
    {
        "id": "msk_impact_50k_2026",
        "cancer_type": "mixed",
        "clinical_attr": "CANCER_TYPE",
    },
    {
        "id": "msk_chord_2024",
        "cancer_type": "mixed",
        "clinical_attr": "CANCER_TYPE",
    },
    {
        "id": "ccle_broad_2019",
        "cancer_type": "mixed",
        "clinical_attr": "CANCER_TYPE",
    },
]

# The three endpoints that dominate wall-clock time in production
HEAVY_ENDPOINTS = [
    "/study/summary/chart/mutated-genes",
    "/study/summary/chart/scatter",
    "/study/summary/chart/km",
]

# All chart endpoints fired when a study dashboard loads
ALL_CHART_ENDPOINTS = [
    "/study/summary/chart/clinical",
    "/study/summary/chart/mutated-genes",
    "/study/summary/chart/cna-genes",
    "/study/summary/chart/scatter",
    "/study/summary/chart/km",
    "/study/summary/chart/data-types",
    "/study/summary/chart/age",
]
