![pixlr-image-generator-6848f506e31846634ceb0225](https://github.com/user-attachments/assets/0beb217a-390e-4817-bc3f-16b52610ec97)

# CSV2ELK
Intended for DFIR and Incident Response, this is a simple tool that uploads CSV files to ELK

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
```
## Example
![image](https://github.com/user-attachments/assets/559a79a9-fc6c-43c6-b11e-2d3a126fcc5a)

## 👤 Author

**Jacob Wilson**  
📧 dfirvault@gmail.com
[https://www.linkedin.com/in/jacob--wilson/](https://www.linkedin.com/in/jacob--wilson/)
