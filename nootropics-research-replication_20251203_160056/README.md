# Nootropics Market Segmentation Research Tool - Replication Package

## Package Information
- **Generated**: 2025-12-03T16:00:56.765607
- **Package Version**: 1.0
- **Total Files**: 23

## Description
Academic research tool for qualitative analysis of Reddit data using the Push-Pull-Mooring (PPM) framework with LLM-assisted thematic coding, following Creswell & Creswell (2023) mixed methods standards.

## Quick Start

### Prerequisites
- Python 3.11 or higher
- PostgreSQL 14+ (optional, SQLite works as fallback)

### Installation

1. **Create virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your API credentials
   ```

4. **Run the application**:
   ```bash
   streamlit run app.py --server.port 5000
   ```

## Package Contents

### Code (`/code`)
- `app.py` - Main Streamlit application
- `database.py` - SQLAlchemy database models
- `modules/` - Analysis modules (Reddit scraper, LLM coder, etc.)
- `utils/` - Helper functions including data anonymization

### Data (`/data`)
- `raw/` - Original Reddit data exports
- `processed/` - Coded and analyzed data
- Place your exported CSV/JSON files here

### Documentation (`/docs`)
- `replication_package_guide.md` - Complete replication instructions
- `zotero_integration_writeup.md` - Citation integration documentation

### Output (`/output`)
- `appendices/` - Thesis appendices (A-G)
- `codebook/` - Code definitions
- Place your generated exports here

## Reproducing the Analysis

### Option A: Using Exported Data (No API keys needed)
1. Load CSV files from `data/processed/` into your preferred software
2. Codebook and coding scheme documented in `output/codebook/`

### Option B: Full Replication
1. Obtain Reddit API credentials
2. Obtain Zotero API credentials (optional)
3. Configure `.env` file
4. Follow collection parameters in Appendix A
5. Run LLM coding with settings in Appendix B

## Verification

To verify package integrity:
```python
python verify_package.py
```

## Software Versions

See `requirements.txt` for exact package versions used.

## Citation

Please see `CITATION.cff` for citation information.

## License

See `LICENSE` file for licensing terms.
- Code: MIT License
- Data: CC-BY-4.0

## Contact

[Your contact information]
