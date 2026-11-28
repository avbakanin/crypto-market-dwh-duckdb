"""Signal research on the USD-M features and labels (stage 3), outside the pipeline.

Reads int_um__features_daily and fct_um_forward_returns from the warehouse read-only,
evaluates a fixed list of signal versions walk-forward (research.config), and writes each
run to data/research/<run_id>/. docs/signal_research.md has the method and the results.
Run from repo root: python -m research.run [--placebo] [--holdout VERSION ...]
"""
