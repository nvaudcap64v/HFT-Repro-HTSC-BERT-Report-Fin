# Financial Analyst Report Sentiment Analysis Based on BERT

**Replicating HuaTai Securities' Quantitative Strategy & Utilized by ByteDance's Trae Campus**

## Overview

This project, initiated by the Fintech Association of Hunan University, replicates the quantitative strategy outlined in HuaTai Securities' research report "*Sentiment Factor Based on BERT for Analyst Reports*". We implemented a crawler to collect analyst report summaries from Sina Finance, amassing **716,834 valid data points**.

Key Achievements:
1.  **Pre-trained** the [HIT Chinese RoBERTa-wwm-ext-large](https://huggingface.co/hfl/chinese-roberta-wwm-ext-large) model (`chinese_roberta_wwm_large_ext_L-24_H-1024_A-16`) on our dataset, achieving an impressive **accuracy of 99.7%**.
2.  Utilized this pre-trained BERT model to score analyst report summaries spanning **January 2008 to December 2009**.
3.  Conducted backtesting based on the sentiment scores, yielding an average **RankIC of -3.98%**.

This project has been recognized and **featured on ByteDance's Trae Campus**.

## Author

*   **Che He** ([@nvaudcap64v](mailto:nvaudcap64v)) - April 8, 2025

## Getting Started

### Prerequisites & Setup

1.  **Model Download:** The project utilizes the pre-trained [HIT Chinese RoBERTa-wwm-ext-large model](https://huggingface.co/hfl/chinese-roberta-wwm-ext-large). Download it before proceeding.
2.  **Data Path Configuration:**
    *   The crawled Sina Finance analyst report summaries are located in the `/sina-report` directory.
    *   **Crucially,** you must update the local path in the `/Source/03-write-to-mongo.py` file (line 11) and other relevant file paths within the `/Source` directory to match your local environment.
3.  **Data Availability:** The `test.csv` file required for pre-training (`/Source/06-pre-train-BERT.py`) is located in the `/Intermediate` directory.

### Execution Order

**Run the scripts in the `/Source` directory sequentially according to their numerical prefix (e.g., `01-...py`, `02-...py`).**

### Important Note on Crawler

*   The web crawler is provided **strictly for the research purposes of this project**. Please use it responsibly and respect Sina Finance's terms of service.

## Limitations & Future Work

1.  **Scope of Data Processed:** Due to computational constraints, processing was limited to data from **2008-2009**. Extending the analysis to cover additional years is a key future direction. Exploring alternative models is also suggested.
2.  **Data Handling Pipeline:** The current process uses `/Source/08-factor-visualize.py` and `/Source/09-pivot-out.py` to convert results to Excel for analysis. This method becomes inefficient with larger datasets. **Future improvement:** Merge `/Source/08-factor-visualize.py`, `/Source/09-pivot-out.py`, and `/Source/11-rankic.py` into a more efficient, scalable processing pipeline.
3.  **Crawler Performance:** Data collection speed was limited by the lack of proxy usage. **Potential Enhancement:** Implement an IP proxy pool and adjust request frequencies to significantly improve data gathering speed.
