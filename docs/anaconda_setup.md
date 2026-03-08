# Anaconda Environment Setup Guide

This guide details how to set up, manage, and run the **NootropicRedditScrapePPM** project using Anaconda (or Miniconda).

## 1. Prerequisites

Ensure you have Anaconda or Miniconda installed on your system.

- [Download Anaconda](https://www.anaconda.com/download)
- [Download Miniconda](https://docs.conda.io/en/latest/miniconda.html) (Lighter version, recommended)

## 2. Setting Up the Environment

Open your terminal (Anaconda Prompt, PowerShell, or Command Prompt) and navigate to the project root directory:

```bash
cd "path\to\NootropicRedditScrapePPM"
```

### Create the Environment

We will create a fresh environment named `nootropic_env` running Python 3.11.

```bash
conda create -n nootropic_env python=3.11 -y
```

### Activate the Environment

You must activate the environment before installing libraries or running the app.

```bash
conda activate nootropic_env
```

### Install Dependencies

This project uses `pip` to manage specific library versions. Ensure you are inside the active `nootropic_env` before running this.

```bash
pip install -r requirements.txt
```

---

## 3. Running the Application

To launch the dashboard, use the `streamlit run` command. **Do not** use `python app.py`.

```bash
streamlit run app.py
```

This will automatically open the application in your default web browser (usually at `http://localhost:8501`).

---

## 4. Common Pitfalls to Avoid ⚠️

Here are the most common mistakes users make when running this project:

### ❌ 1. Running `python app.py`

**Error:** Nothing happens, or the script exits immediately.
**Solution:** Streamlit apps must be run with the `streamlit run` command, not the standard python interpreter.
- **Wrong:** `python app.py`
- **Right:** `streamlit run app.py`

### ❌ 2. Forgetting to Activate the Environment

**Error:** `ModuleNotFoundError: No module named 'streamlit'` or similar import errors.
**Solution:** You are likely in your "base" environment or global python scope. Always run:

```bash
conda activate nootropic_env
```

Look for `(nootropic_env)` at the start of your command line prompt.

### ❌ 3. Wrong Working Directory

**Error:** `FileNotFoundError: [Errno 2] No such file or directory: 'database.py'`
**Solution:** You must run the command from the *root* folder of the project (where `app.py` is located).
- Check your location with `pwd` (Mac/Linux) or `dir` (Windows).
- `cd` into the correct folder before running the app.

### ❌ 4. Installing with `conda install` instead of `pip`

**Error:** Version conflicts or package not found.
**Solution:** While we use Conda for the *environment*, we use `pip` for the *packages* to ensure exact compatibility with the `requirements.txt`.
- **Preferred:** `pip install -r requirements.txt` inside the conda env.

### ❌ 5. PowerShell "Command Not Found"

**Error:** PowerShell doesn't recognize `conda`.
**Solution:** If you are using standard PowerShell instead of the "Anaconda Powershell Prompt", you might need to initialize it first:

```bash
conda init powershell
```

Then close and reopen PowerShell.

---

## Quick Reference (Cheatsheet)

| Action | Command |
| :--- | :--- |
| **Start App** | `conda activate nootropic_env` <br> `streamlit run app.py` |
| **Update Deps** | `pip install -r requirements.txt` |
| **Stop App** | `Ctrl + C` in the terminal |
| **Exit Env** | `conda deactivate` |
