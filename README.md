# OpenNeuro Data Processing Application

A desktop application for downloading, processing, and analyzing intracranial EEG (iEEG) data from OpenNeuro in BIDS format. The application provides an interface for selecting specific datasets and files, processing them with various neuroscientific analyses, and storing results in AWS S3.

## Features

- Download iEEG datasets from OpenNeuro using DataLad
- Select specific subjects and files through a GUI interface
- Process iEEG data with multiple analysis methods:
  - Power spectral analysis
  - Frequency band decomposition (delta, theta, alpha, beta, gamma)
  - Connectivity analysis
- Secure AWS S3 integration for result storage
- BIDS format compatibility

## Prerequisites

- Python 3.8 or higher
- git-annex >= 8.20200309
- AWS CLI configured with appropriate credentials
- Sufficient storage space for downloaded datasets

### Installing Dependencies

1. Install git-annex:
```bash
# macOS (with Homebrew)
brew install git-annex

# Ubuntu/Debian
sudo apt-get install git-annex
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS CLI with your credentials:
```bash
aws configure
```

## Directory Structure

```
openneuro_app/
│── main.py               # Entry point for the application
│── gui.py                # GUI with PyQt6
│── fetch_data.py         # API handling for OpenNeuro
│── s3_upload.py          # Secure AWS S3 handling
│── process_ieeg.py       # Neuroscientific processing tasks
│── config.py             # Configuration settings
│── requirements.txt      # Dependencies
```

## Usage

1. Start the application:
```bash
python main.py
```

2. Enter an OpenNeuro dataset accession number (e.g., 'ds005691')

3. Click "Fetch Dataset" to download the dataset metadata

4. Select the files you want to process from the file tree
   - Required files include:
     - `*_ieeg.tsv` (data)
     - `*_ieeg.json` (metadata)
     - `*_channels.tsv` (channel information)
     - `*_events.tsv` (if available)

5. Click "Process Selected Files" to run the analysis

6. Results will be automatically uploaded to your configured AWS S3 bucket

## Processing Pipeline

The application performs the following analyses on iEEG data:

1. Time-Frequency Analysis
   - Power spectral density using Welch's method
   - Frequency band decomposition

2. Connectivity Analysis
   - Channel-wise connectivity matrices
   - Phase-locking value calculations

3. Signal Quality and Preprocessing
   - Artifact removal
   - Band-specific filtering

## Output Structure

Processed data is stored in your AWS S3 bucket with the following structure:
```
processed/
└── dataset_id/
    ├── processing_results_TIMESTAMP.json
    └── source_files/
        └── [original files]
```

## Security

- AWS credentials are handled through AWS CLI configuration
- Temporary files are cleaned up after processing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request


## Acknowledgments

- OpenNeuro for providing the dataset infrastructure
- DataLad for dataset version control
- MNE-Python for iEEG processing capabilities