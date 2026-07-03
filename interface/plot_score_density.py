import logging

import matplotlib.pyplot as plt
import pandas as pd

logger = logging.getLogger(__name__)


def build_score_density_plot(scores, threshold=0.98):
    scores_series = pd.Series(scores, name="score")

    stats = (
        f"n = {len(scores_series)}\n"
        f"mean:   {scores_series.mean():.5f}\n"
        f"median: {scores_series.median():.5f}\n"
        f"max:    {scores_series.max():.5f}\n"
        f"fraud count: {(scores_series > threshold).sum()}"
    )
    
    fig, ax = plt.subplots(figsize=(8, 5))

    axis_color = "#787878"
    text_color = "#4B4B4B"
    hist_color = plt.get_cmap("tab10")(0)
    threshold_color = "#d62728"

    ax.hist(scores_series, bins=40, color=hist_color, edgecolor="white")

    ax.axvline(
        threshold,
        color=threshold_color,
        linestyle="--",
        linewidth=1,
        label=f"threshold = {threshold:.3f}",
    )
    ax.legend(
        loc="upper right",
        bbox_to_anchor=(0.97, 1.0),
        frameon=False,
        prop={"family": "monospace", "size": 8},
    )

    ax.text(
        0.41,
        0.95,
        stats,
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=8,
        family="monospace",
        color=axis_color,
    )

    ax.set_title(
        "Распределение скоров",
        fontweight="bold",
        fontsize=14,
        color=text_color,
        pad=16,
    )

    ax.set_xlim(0, 1)
    ax.set_xlabel("Score")
    ax.set_ylabel("Частота")
    ax.grid(ls=":", alpha=0.5)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(axis_color)
    ax.spines["bottom"].set_color(axis_color)
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_linewidth(0.8)

    ax.tick_params(axis="both", colors=axis_color)
    ax.xaxis.label.set_color(text_color)
    ax.yaxis.label.set_color(text_color)

    return fig