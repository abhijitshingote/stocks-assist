"""Agents that make up the screening chain.

Each agent is a callable class with a single `.run(input_data) -> output_data`
method. Inputs and outputs are JSON-serializable dicts so any stage can be
swapped out, replayed, or composed differently.
"""
