"""GlobalRetail ETL package.

Modules:
    config     - centralized configuration loaded from environment
    logger     - structured JSON logging + timing decorator
    extract    - data acquisition (CSV + REST APIs) with retry logic
    transform  - cleaning, normalization, enrichment, FX conversion
    load       - idempotent bulk upsert to MongoDB Atlas + watermark update
"""
