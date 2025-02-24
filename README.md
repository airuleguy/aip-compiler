# AIP Uruguay PDF Downloader

This Python script downloads and compiles Aeronautical Information Publication (AIP) PDFs from the DINACIA (Dirección Nacional de Aviación Civil e Infraestructura Aeronáutica) website for Uruguay into a single document. It handles multiple groups of PDFs, including both iterable (e.g., `Gen0.pdf`, `Gen1.pdf`) and fixed (e.g., `AIPAMDT.pdf`) files, searching for the most recent versions within a 2-year window.

## Features
- Downloads PDFs from five groups: Heading, General, EnRoute, Aerodromes, Additional_Aerodromes, and Amendment.
- Iterates backwards from the current date (month-by-month, year-by-year) to find the latest valid files, up to 2 years back.
- Uses parallel downloads (10 workers) for efficiency while preserving order in the final PDF.
- Merges all downloaded PDFs into a single file, e.g., `aip_uruguay_compiled_2025-02.pdf`.
- Logs all actions to `aip_download.log` for debugging and verification.

## Prerequisites
- **Python 3.6+**: Required to run the script.
- **Libraries**:
  - `requests`: For HTTP requests.
  - `PyPDF2`: For merging PDFs.
  - Install them via pip:
    ```bash
    pip install requests PyPDF2
