# Dependencies and licenses

The full resolved Windows x64 and Python 3.12 dependency set is in [requirements.lock.txt](requirements.lock.txt). Versions below were verified in the project environment. The installed distributions contain their applicable license and third-party notices.

| Dependency | Version | License |
| --- | --- | --- |
| NumPy | 2.4.6 | BSD-3-Clause with bundled 0BSD, MIT, Zlib and CC0-1.0 notices |
| pandas | 2.3.3 | BSD-3-Clause |
| PyArrow | 26.0.1 | Apache-2.0 |
| DuckDB | 2.0.0 | MIT |
| HTTPX | 0.28.1 | BSD-3-Clause |
| scikit-learn | 1.9.1 | BSD-3-Clause |
| VectorBT | 1.0.0 | Apache-2.0 with Commons Clause |
| Numba | 0.68.1 | BSD and bundled notices |
| Plotly | 6.5.2 | MIT |
| Streamlit | 1.67.0 | Apache-2.0 |
| Pydantic | 2.14.3 | MIT |
| PyYAML | 6.0.3 | MIT |
| Joblib | 1.6.0 | BSD-3-Clause |
| threadpoolctl | 3.8.0 | BSD-3-Clause |
| tzdata | 2026.6 | Apache-2.0 |
| pytest | 9.2.0 | MIT |

VectorBT's Commons Clause is relevant to any later redistribution or commercial service that uses the adapter. [VectorBT license](https://github.com/polakowo/vectorbt/blob/master/LICENSE.md)

Plotly 6.5.2 retains the template properties VectorBT 1.0.0 registers at import. Training and simulation run on CPU.
