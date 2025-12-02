# Open Science Framework (OSF) Setup Guide

## Why OSF?

The Open Science Framework (osf.io) is ideal for academic research sharing because:

- **Free**: Unlimited public projects, 5GB private storage
- **DOI Assignment**: Get a citable DOI for your materials
- **Version Control**: Track changes over time
- **Integration**: Links with GitHub, Dropbox, Google Drive
- **Preregistration**: Support for study preregistration
- **Institutional Trust**: Widely recognized in academia

---

## Step 1: Create an OSF Account

1. Go to [https://osf.io](https://osf.io)
2. Click "Sign Up" in the top right
3. Use your institutional email for credibility
4. Verify your email address

---

## Step 2: Create a New Project

1. Click "My Projects" → "Create Project"
2. Fill in project details:

   **Title**: 
   ```
   Replication Materials: Natural Cognitive Supplement Market Segmentation Using PPM Framework
   ```

   **Description**:
   ```
   This repository contains the complete replication package for a mixed-methods 
   study analyzing Reddit discourse on natural cognitive supplements (nootropics) 
   using the Push-Pull-Mooring (PPM) framework. Materials include: research tool 
   source code, de-identified data exports, LLM-assisted coding outputs, codebook, 
   inter-coder reliability metrics, and thesis appendices (A-G).
   
   The analysis follows Creswell & Creswell (2023) mixed methods standards with 
   complete audit trail documentation for methodological transparency.
   ```

3. Set visibility:
   - **Private** during research (can share with specific collaborators)
   - **Public** after publication/defense

4. Click "Create"

---

## Step 3: Organize Your Project Structure

Create the following components (sub-projects):

### Component 1: Source Code
- Title: "Research Tool Source Code"
- Description: "Streamlit-based analysis tool with Reddit scraper, LLM coder, and export modules"
- **Tip**: Link to GitHub repository instead of uploading directly

### Component 2: Data
- Title: "De-identified Data Exports"
- Description: "Reddit posts and comments with coding applied, usernames removed"
- Upload: CSV and JSON exports

### Component 3: Documentation
- Title: "Methodology Documentation"
- Description: "Codebook, collection parameters, and analysis procedures"
- Upload: Appendices A-G, codebook exports

### Component 4: Outputs
- Title: "Analysis Outputs"
- Description: "Topic models, reliability metrics, and visualizations"
- Upload: Generated reports and figures

---

## Step 4: Upload Your Files

### Preparing Files for Upload

1. Run the packaging script:
   ```bash
   python package_for_sharing.py
   ```

2. Add your exported data to the package:
   - Export data from the app (Data Export module)
   - Export all thesis appendices (Thesis Export module)
   - Place files in appropriate folders

3. Create the final ZIP:
   ```bash
   cd nootropics-research-replication_[timestamp]/
   zip -r ../replication-package.zip .
   ```

### Uploading to OSF

1. Navigate to your project
2. Click "Files" in the left sidebar
3. Drag and drop files or click "Upload"
4. Organize into folders matching your structure

### Recommended File Organization on OSF

```
osf-project/
├── README.md                    # Project overview
├── LICENSE.md                   # License terms
│
├── code/                        # Or link to GitHub
│   ├── app.py
│   ├── requirements.txt
│   └── modules/
│
├── data/
│   ├── raw_exports/
│   │   ├── reddit_posts.csv
│   │   └── reddit_comments.csv
│   ├── coded_data/
│   │   ├── coded_posts.csv
│   │   └── coding_summary.json
│   └── DATA_DICTIONARY.md
│
├── documentation/
│   ├── Appendix_A_Collection_Parameters.md
│   ├── Appendix_B_Codebook.md
│   ├── Appendix_C_Audit_Trail.md
│   ├── Appendix_D_Topic_Models.md
│   ├── Appendix_E_Reliability.md
│   ├── Appendix_F_Edge_Cases.md
│   └── Appendix_G_Literature_Links.md
│
└── outputs/
    ├── figures/
    ├── tables/
    └── reliability_reports/
```

---

## Step 5: Link GitHub Repository (Recommended)

Instead of uploading code directly, link to GitHub:

1. Go to your OSF project settings
2. Click "Add-ons" in the left menu
3. Enable "GitHub"
4. Authorize OSF to access your GitHub
5. Select your repository

**Benefits**:
- Code stays version-controlled
- Updates automatically sync
- Maintains complete commit history

---

## Step 6: Add Collaborators (Optional)

For committee members or co-authors:

1. Go to project settings → "Contributors"
2. Add by email or OSF username
3. Set permissions:
   - **Administrator**: Full control
   - **Read+Write**: Can edit
   - **Read**: View only

---

## Step 7: Generate a DOI

**Important**: Only do this when you're ready to "freeze" a version

1. Go to project settings
2. Click "Create DOI"
3. Choose what to register:
   - Entire project
   - Specific component
4. Confirm registration

**The DOI is permanent** - you cannot delete the project after registration

### Pre-DOI Checklist

- [ ] All files uploaded and organized
- [ ] README complete with instructions
- [ ] License specified
- [ ] Data de-identified
- [ ] Citation information included
- [ ] Collaborators added

---

## Step 8: Make Public (When Ready)

1. Go to project settings
2. Click "Make Public"
3. Choose visibility for each component
4. Confirm

### Timing Recommendations

| Stage | Visibility |
|-------|------------|
| During data collection | Private |
| During analysis | Private (share with committee) |
| After defense | Public |
| After publication | Public with DOI |

---

## Step 9: Add to Your Thesis/Paper

Include in your methodology section:

```
Replication materials, including source code, de-identified data, and 
complete analytical outputs, are available at the Open Science Framework: 
https://osf.io/[YOUR-PROJECT-ID]/ (DOI: 10.17605/OSF.IO/[YOUR-ID])
```

Add to your references:

```
[Your Name]. (2025). Replication Materials: Natural Cognitive Supplement 
Market Segmentation Using PPM Framework [Data set and code]. OSF. 
https://doi.org/10.17605/OSF.IO/[YOUR-ID]
```

---

## Alternative: Zenodo Integration

OSF can automatically archive to Zenodo:

1. Enable Zenodo add-on in project settings
2. Connect your Zenodo account
3. Zenodo creates a backup with its own DOI
4. Updates sync automatically

**Benefits of dual archiving**:
- Zenodo backed by CERN (long-term preservation)
- Two independent DOIs for citation flexibility
- Redundant storage for data safety

---

## Sharing Best Practices

### For Reviewers/Committee

Share a private link:
1. While project is private, click "Share"
2. Generate a view-only link
3. Send to reviewers
4. They can access without OSF account

### For Other Researchers

After making public:
1. Share the DOI link
2. Include citation instructions
3. Specify which license applies
4. Provide contact info for questions

### License Recommendations

| License | Best For |
|---------|----------|
| CC-BY 4.0 | Data and documentation |
| MIT | Code |
| CC0 | Maximum reuse (no attribution required) |

---

## Troubleshooting

### Large Files (>5GB)

- Split data into smaller chunks
- Use OSF Storage add-on (paid)
- Link to external storage (Google Drive, Dropbox)

### Sensitive Data

- Never upload identifiable information
- Use pseudonymization scripts
- Document redaction in methodology

### Version Updates

- OSF tracks all file versions automatically
- Use descriptive commit messages
- Consider tagging major versions

---

## Quick Reference

| Task | Location |
|------|----------|
| Create project | My Projects → Create |
| Upload files | Files → Upload |
| Add collaborators | Settings → Contributors |
| Link GitHub | Settings → Add-ons → GitHub |
| Generate DOI | Settings → Create DOI |
| Make public | Settings → Make Public |
| Share privately | Share button → View-only link |

---

## Support Resources

- OSF Guides: https://help.osf.io/
- OSF Support: support@osf.io
- Community Forum: https://groups.google.com/g/openscienceframework
