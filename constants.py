# constants.py
# PabstBrain shared display constants

WATERFALL_LABELS = {
    "wholesale":    "Full Price Gross Sales",
    "price_red":    "Discounts from Preferred Pricing",
    "promos":       "Promos / Samples",
    "order_disc":   "Order-Level Discounts",
    "credit_memo":  "Credit Memos",
    "net_revenue":  "Net Revenue",
}

KPI_LABELS = {
    "gross_sales":      "Full Price Gross Sales",
    "price_reduction":  "Discounts from Preferred Pricing",
    "promos_samples":   "Promos / Samples",
    "order_discounts":  "Order-Level Discounts",
    "credit_memos":     "Credit Memos",
    "net_revenue":      "Net Revenue",
    "units_sold":       "Units Sold",
    "avg_net_per_unit": "Avg Net per Unit",
}

AR_BUCKET_LABELS = {
    "Current (0-15)":    "Current (0-15 days)",
    "Early (16-30)":     "Early (16-30 days)",
    "Warning (31-45)":   "Warning (31-45 days)",
    "Late (46-60)":      "Late (46-60 days)",
    "Serious (61-90)":   "Serious (61-90 days)",
    "Collections (90+)": "Collections (90+ days)",
}

BRAND_LABELS = {
    "St Ides": "St. Ides",
    "NYF":     "Not Your Father's",
    "PBR":     "Pabst Blue Ribbon",
    "Other":   "Other",
}

ORDER_CATEGORY_LABELS = {
    "REVENUE":               "Revenue",
    "PENNY_OUT_PROMO":       "Promos / Samples",
    "INTERCOMPANY_TRANSFER": "Intercompany Transfer",
    "CONSIGNMENT_LEGACY":    "Legacy Consignment",
    "CANCELLED":             "Cancelled",
    "REJECTED":              "Rejected",
    "SCHEDULED":             "Scheduled",
    "IN_TRANSIT":            "In Transit",
    "UNSCHEDULED":           "Unscheduled",
    "OTHER":                 "Other",
}

DASHBOARD_VERSION = "v3.0"
