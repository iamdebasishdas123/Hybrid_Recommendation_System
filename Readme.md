
---

# Recommendation System 

## Overview

This project implements a **Recommendation System** using data ingestion, preprocessing, and recommendation generation workflows.

---

## Features

- **Data Ingestion**: Featch the data from the given link and store the data in raw folder.
- **Data Preprocessing**: Cleans and merges data into a unified format for modeling.
- **Recommendation Engine**: Generates recommendations based on user data and behavior.

---

## Key Features:
- Content-based filtering: Recommending videos similar to those the user has viewed or liked.
- Collaborative filtering: Leveraging similar user preferences to enhance recommendations.
- Hybrid models: Combining content-based and collaborative filtering for improved accuracy.
- Include a mechanism to recommend videos for new users without prior interaction history (Based on Tranding videos,most viewed videos,Most liked videos and also Based on mood).

## Project Structure

```plaintext                  
data/
├── preprocessed/        # Preprocessed data files
│   └── merged_data.csv  # Cleaned and merged dataset
├── raw/                 # Raw input datasets
│   ├── get_all.csv
│   ├── inspire.csv
│   ├── liked.csv
│   ├── rating.csv
│   └── view.csv
logs/                    # Logs for debugging
Notebook/
├── merge_data.ipynb     # Data merging notebook
├── Recommendation.ipynb # Recommendation engine notebook
data_ingestion.py        # Script for data ingestion
data_preprocessing.py    # Script for data preprocessing
recommendation.py        # Script for generating recommendations
README.md                # Project documentation
requirements.txt         # Dependencies
```

---

## Setup Instructions

Follow these steps to set up and run the project:

### Prerequisites

Ensure the following are installed:
- Python 3.8 or higher
- `pip` (Python package manager)
- Git

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

---

## Running the Project


1. **Local Execution:**
   Alternatively, run scripts locally:
   ```bash
   python data_ingestion.py
   python data_preprocessing.py
   python recommendation.py
   ```

2. **Results:**

   ```bash
   recommendations.json
   ```
   - It can give the Recommened videos basis of content and user experience.
   - Include a mechanism to recommend videos for new users without prior interaction history (Based on Tranding videos,most viewed videos,Most liked videos and also Based on mood).

---
