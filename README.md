This repository contains the replication script for the working paper:

**“The Wrong Shareholders: Why Merger Arbitragers Now Control M&A Approval.”**

The script studies what happens to who owns a company **after a merger is announced but before shareholders vote**.

In short, it traces whether the shareholders who vote on a deal are the same shareholders who owned the company when the deal was signed. To do this, it combines merger data, stock data, and institutional holdings data from WRDS and then runs a sequence of event-study and decomposition analyses.

his project relies on licensed WRDS datasets. You will need:
- a working WRDS account,
- permission to query the required databases/tables,
- an active R connection object named `wrds` before running the script.

The script expects WRDS data sources such as SDC M&A, CRSP, Thomson 13F, and factor tables.

## What the script produces

Running the script creates intermediate and final outputs including:
- cleaned merger samples,
- target/acquirer/control panels,
- institutional ownership time series,
- flow decomposition outputs (entrants, exiters, stayers),
- replacement and concentration metrics,
- risk-transformation outputs,
- saved `.rds` objects and selected `.png` figures.

## Quick start

1. Open the project in R/RStudio.
2. Install required packages (the script checks packages at runtime).
3. Create and test your WRDS connection, assigning it to `wrds`.
4. Run `Replication Script.R` from top to bottom.
5. Review generated `.rds` and figure files in the repository folder.

## Notes on reproducibility

- Results depend on WRDS table versions and your data entitlements.
- Some matching and sampling steps involve randomization/order effects.

## File overview

- `Replication Script.R` — main end-to-end analysis script.
- `README.md` — this guide.

## Citation

If you use or adapt this code, please cite the associated paper:

*The Wrong Shareholders: Why Merger Arbitragers Now Control M&A Approval.*
