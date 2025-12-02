# Replication Package Guide

## How to Download This Project

### From Replit

1. **Export as ZIP**: In the file explorer, you can download your project as a ZIP file
2. **Download Individual Folders**: Navigate to specific folders and download them directly

### Recommended Package Structure

After downloading, organize for sharing:

```
nootropics-research-tool/
├── README.md                    # Setup instructions
├── LICENSE                      # MIT or appropriate license
├── requirements.txt             # Python dependencies (already exists)
├── .env.example                 # Template for environment variables
│
├── code/
│   ├── app.py                   # Main application
│   ├── database.py              # Database models
│   └── modules/                 # All analysis modules
│       ├── reddit_scraper.py
│       ├── llm_coder.py
│       ├── zotero_manager.py
│       └── ...
│
├── data/
│   ├── raw/                     # Original exported data (CSV/JSON)
│   └── processed/               # Coded data exports
│
├── output/
│   ├── appendices/              # Generated thesis appendices (A-G)
│   ├── codebook/                # Exported codebook
│   └── reliability/             # Inter-coder reliability reports
│
├── docs/
│   ├── methodology_writeup.md   # This documentation
│   ├── zotero_integration.md    # Zotero feature documentation
│   └── replication_package.md   # This guide
│
└── database/
    └── research_data.db         # SQLite database backup (if used)
```

## Creating the Replication Package

### Step 1: Export Your Data

Before packaging, export all research data from within the app:

1. Go to **Data Export & Audit** module
2. Export collected data as CSV and JSON
3. Export coded data with all metadata
4. Export codebook definitions

### Step 2: Export Thesis Appendices

Generate all appendices for documentation:

1. Go to **Thesis Exports** module
2. Download each appendix (A through G)
3. Save the Complete Methodology Chapter

### Step 3: Database Backup

For complete replication:

```bash
# If using PostgreSQL, export to SQL dump
pg_dump $DATABASE_URL > database/research_backup.sql

# If using SQLite, simply copy the .db file
cp research.db database/research_data.db
```

### Step 4: Document Environment Variables

Create `.env.example` (without actual values):

```
# Reddit API (required for data collection)
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
REDDIT_USER_AGENT=your_user_agent_here

# Zotero API (required for citation integration)
ZOTERO_API_KEY=your_api_key_here
ZOTERO_LIBRARY_ID=your_library_id_here

# Database (PostgreSQL or SQLite)
DATABASE_URL=postgresql://user:pass@host:port/dbname
```

### Step 5: Create Comprehensive README

Your README should include:

```markdown
# Nootropics Market Segmentation Research Tool

## Description
Academic research tool for qualitative analysis of Reddit data using 
the Push-Pull-Mooring (PPM) framework with LLM-assisted thematic coding.

## Authors
[Your Name], [Institution], [Email]

## Publication
[Citation for associated thesis/paper]
DOI: [if available]

## Software Requirements
- Python 3.11+
- PostgreSQL 14+ (or SQLite for local use)
- See requirements.txt for Python packages

## Installation

1. Clone/download this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and add your credentials
4. Initialize database:
   ```
   python -c "from database import init_db; init_db()"
   ```
5. Run the application:
   ```
   streamlit run app.py --server.port 5000
   ```

## Reproducing Results

### Using Exported Data (No API keys needed)
1. Import the CSV files from `data/processed/`
2. Load into your preferred analysis software (NVivo, MAXQDA, R, Python)

### Full Replication (API keys required)
1. Obtain Reddit API credentials
2. Obtain Zotero API credentials  
3. Follow collection parameters documented in Appendix A
4. Run LLM coding with settings documented in Appendix B

## File Descriptions

| File/Folder | Description |
|-------------|-------------|
| `app.py` | Main Streamlit application |
| `modules/` | Analysis module implementations |
| `data/raw/` | Original Reddit exports |
| `data/processed/` | Coded and analyzed data |
| `output/appendices/` | Thesis appendix exports |

## Collection Parameters

See `output/appendices/Appendix_A_*.md` for:
- Subreddits sampled
- Date range of collection
- Inclusion/exclusion criteria
- NSFW handling decisions

## Codebook

See `output/appendices/Appendix_B_*.md` for complete codebook with:
- PPM category definitions
- Code definitions and examples
- Emergent theme documentation

## Ethical Considerations

- All data from publicly accessible subreddits
- Usernames anonymized in exports
- Compliant with Reddit API Terms of Service
- IRB approval: [Protocol number if applicable]

## License
[Specify license - MIT, CC-BY, etc.]

## Citation
If you use this tool or data, please cite:
[Your citation in APA format]
```

## Where to Share

### Academic Repositories (Recommended)

| Platform | Best For | DOI Support | Storage Limit |
|----------|----------|-------------|---------------|
| **OSF** (osf.io) | Social science research | Yes | 5GB free |
| **Zenodo** (zenodo.org) | Any research | Yes | 50GB free |
| **Harvard Dataverse** | Quantitative data | Yes | 2.5GB free |
| **Figshare** | Supplementary materials | Yes | 20GB free |

### Code Repositories

| Platform | Best For | Notes |
|----------|----------|-------|
| **GitHub** | Code sharing | Add data to OSF/Zenodo |
| **GitLab** | Private repos | Self-hosted option |

### Recommended Approach

1. **Code + Documentation** → GitHub (version controlled)
2. **Data + Exports** → OSF or Zenodo (gets DOI for citation)
3. **Link Both** → Reference each in README files

## Pre-Sharing Checklist

### Data Protection
- [ ] All Reddit usernames removed or pseudonymized
- [ ] No personally identifiable information in exports
- [ ] NSFW content handled per your ethics protocol
- [ ] IP addresses and location data removed

### Completeness
- [ ] All appendices (A-G) exported
- [ ] Codebook with examples included
- [ ] Collection parameters documented
- [ ] Software versions recorded

### Reproducibility
- [ ] requirements.txt up to date
- [ ] .env.example created (no real credentials)
- [ ] README with step-by-step instructions
- [ ] All paths are relative, not absolute

### Legal/Ethical
- [ ] LICENSE file included
- [ ] Reddit API ToS compliance noted
- [ ] IRB information included (if applicable)
- [ ] Citation instructions provided

## Version Information

Record this for reproducibility:

```python
# Generate with: python -c "import pkg_resources; print('\n'.join([f'{p.key}=={p.version}' for p in pkg_resources.working_set]))"

Python: 3.11.x
streamlit==1.x.x
praw==7.x.x
pyzotero==1.x.x
sqlalchemy==2.x.x
pandas==2.x.x
openai==1.x.x
# ... etc
```

## Support

For questions about replication:
- Open an issue on GitHub
- Contact: [your email]
- See documentation in `docs/` folder
