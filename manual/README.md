# laupy documentation

- `laupy_lautools_manual.pdf` – user manual for the laupy CLI tools and lautools-browser
- `laupy_api_reference.pdf` – API reference for the laupy Python package

This directory is the master copy of the LaTeX sources. Build the PDFs with
[tectonic](https://tectonic-typesetting.github.io/):

```bash
tectonic laupy_lautools_manual.tex
tectonic laupy_api_reference.tex
```

The lautools repository keeps only a copy of `laupy_lautools_manual.pdf` (in its `manual/`);
copy the rebuilt PDF there when the user manual changes.
