# CSV2ELK
Intended for DFIR and Incident Response, this is a simple tool that converts CSV files to ELK

# CSV2ELK v0.1
**Bulk CSV-to-Elasticsearch Importer for DFIR & Threat Hunting**  
*Author: Jacob Wilson | [dfirvault@gmail.com](mailto:dfirvault@gmail.com)*  

![Elasticsearch+CSV](https://img.shields.io/badge/Elasticsearch-Data_Loader-blue) 
![License](https://img.shields.io/badge/License-MIT-green)  
*"Ingest forensic logs, IOC feeds, or threat data into ELK with one click."*

---

## 🔥 Features
- **Drag-and-drop CSV ingestion** to Elasticsearch/OpenSearch  
- **Auto-mapping** for timestamps, IPs, and forensic fields  
- **Configurable credentials** (saved securely in `config.txt`)  
- **DFIR-optimized**: Handles malware logs, firewall data, and SIEM exports  
- **Smart timestamp detection** (supports Unix epoch, ISO8601, and more)  

## 📦 Installation
```bash
# Requires Python 3.8+
pip install pandas requests tqdm
