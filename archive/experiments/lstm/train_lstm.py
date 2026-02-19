"""
LSTM model for stock price prediction.

Usage:
    python train_lstm.py
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import torch
import torch.nn as nn
import matplotlib.pyplot as plt
import os
from typing import Tuple

# ============== Configuration ==============
SYMBOL = 'SPY'
DATA_FILE = 'price_data.csv'

# Model hyperparameters
SEQUENCE_LENGTH = 10
PREDICTION_HORIZON = 1
USE_RETURNS = True

# Training parameters
TEST_SPLIT_RATIO = 0.2
EPOCHS = 50
BATCH_SIZE = 16
HIDDEN_SIZE = 50
NUM_LAYERS = 2
LEARNING_RATE = 0.001


# ============== Data Functions ==============
def load_data(file_path: str, symbol: str) -> pd.DataFrame:
    """Load and filter data for a specific symbol."""
    df = pd.read_csv(file_path)
    df = df[df['symbol'] == symbol].copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.set_index('date', inplace=True)
    return df


def prepare_features(df: pd.DataFrame, use_returns: bool) -> np.ndarray:
    """Prepare feature data (returns or raw prices)."""
    prices = df['close_price'].values
    
    if use_returns:
        # Daily percentage returns
        returns = np.diff(prices) / prices[:-1]
        return returns.reshape(-1, 1)
    else:
        return prices.reshape(-1, 1)


def create_sequences(data: np.ndarray, seq_length: int, 
                     horizon: int) -> Tuple[np.ndarray, np.ndarray]:
    """Create input sequences and targets for LSTM."""
    X, y = [], []
    for i in range(len(data) - seq_length - horizon + 1):
        X.append(data[i:(i + seq_length), 0])
        y.append(data[i + seq_length + horizon - 1, 0])
    return np.array(X), np.array(y)


def prepare_train_test_data(data: np.ndarray, seq_length: int, horizon: int,
                            test_ratio: float, use_returns: bool) -> Tuple:
    """Split data chronologically and create sequences."""
    split_idx = int(len(data) * (1 - test_ratio))
    
    train_data = data[:split_idx]
    test_data = data[split_idx - seq_length - horizon + 1:]
    
    # Scale data - fit only on training
    scaler = StandardScaler() if use_returns else MinMaxScaler(feature_range=(0, 1))
    scaled_train = scaler.fit_transform(train_data)
    scaled_test = scaler.transform(test_data)
    
    # Create sequences
    X_train, y_train = create_sequences(scaled_train, seq_length, horizon)
    X_test, y_test = create_sequences(scaled_test, seq_length, horizon)
    
    # Convert to tensors
    X_train = torch.tensor(X_train, dtype=torch.float32).unsqueeze(-1)
    y_train = torch.tensor(y_train, dtype=torch.float32).unsqueeze(-1)
    X_test = torch.tensor(X_test, dtype=torch.float32).unsqueeze(-1)
    y_test = torch.tensor(y_test, dtype=torch.float32).unsqueeze(-1)
    
    return X_train, y_train, X_test, y_test, scaler, split_idx


# ============== Model ==============
class LSTMModel(nn.Module):
    def __init__(self, input_size: int = 1, hidden_size: int = 50,
                 num_layers: int = 2, output_size: int = 1):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size, hidden_size, num_layers=num_layers,
            batch_first=True, dropout=0.2 if num_layers > 1 else 0
        )
        self.linear = nn.Linear(hidden_size, output_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        lstm_out, _ = self.lstm(x)
        return self.linear(lstm_out[:, -1, :])


def train_model(model: LSTMModel, X_train: torch.Tensor, y_train: torch.Tensor,
                epochs: int, batch_size: int, lr: float) -> list:
    """Train the LSTM model."""
    loss_fn = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    losses = []
    
    for epoch in range(epochs):
        model.train()
        indices = torch.randperm(len(X_train))
        X_shuffled = X_train[indices]
        y_shuffled = y_train[indices]
        
        epoch_loss = 0
        n_batches = 0
        
        for i in range(0, len(X_train), batch_size):
            batch_X = X_shuffled[i:i + batch_size]
            batch_y = y_shuffled[i:i + batch_size]
            
            optimizer.zero_grad()
            pred = model(batch_X)
            loss = loss_fn(pred, batch_y)
            loss.backward()
            optimizer.step()
            
            epoch_loss += loss.item()
            n_batches += 1
        
        avg_loss = epoch_loss / n_batches
        losses.append(avg_loss)
        
        if epoch % 10 == 0:
            print(f"Epoch {epoch:3d}: loss = {avg_loss:.6f}")
    
    return losses


def evaluate_model(model: LSTMModel, X_test: torch.Tensor, y_test: torch.Tensor,
                   scaler, use_returns: bool) -> dict:
    """Evaluate model performance."""
    model.eval()
    with torch.no_grad():
        predictions = model(X_test).numpy()
    
    pred_inv = scaler.inverse_transform(predictions)
    actual_inv = scaler.inverse_transform(y_test.numpy())
    
    rmse = np.sqrt(np.mean((pred_inv - actual_inv) ** 2))
    mae = np.mean(np.abs(pred_inv - actual_inv))
    
    if use_returns:
        # Directional accuracy for returns
        dir_acc = np.mean((pred_inv > 0) == (actual_inv > 0)) * 100
        # Naive baseline: predict zero return
        naive_pred = np.zeros_like(actual_inv)
    else:
        dir_acc = None
        # Naive baseline: predict last known price
        naive_pred = scaler.inverse_transform(X_test[:, -1, :].numpy())
    
    naive_rmse = np.sqrt(np.mean((naive_pred - actual_inv) ** 2))
    
    return {
        'rmse': rmse,
        'mae': mae,
        'dir_acc': dir_acc,
        'naive_rmse': naive_rmse,
        'predictions': pred_inv,
        'actuals': actual_inv
    }


def plot_results(metrics: dict, losses: list, symbol: str, 
                 use_returns: bool, output_dir: str):
    """Plot predictions and training loss."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Predictions vs Actuals
    ax1 = axes[0]
    ax1.plot(metrics['actuals'], label='Actual', color='blue', linewidth=2)
    ax1.plot(metrics['predictions'], label='Predicted', color='red', 
             linewidth=2, linestyle='--')
    ax1.set_title(f'{symbol} {"Returns" if use_returns else "Price"} Prediction')
    ax1.set_xlabel('Test Sample')
    ax1.set_ylabel('Return' if use_returns else 'Price ($)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Training Loss
    ax2 = axes[1]
    ax2.plot(losses, color='green', linewidth=2)
    ax2.set_title('Training Loss')
    ax2.set_xlabel('Epoch')
    ax2.set_ylabel('MSE Loss')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{symbol}_prediction.png'))
    plt.show()


def save_model(model: LSTMModel, metrics: dict, config: dict, output_dir: str):
    """Save trained model and metadata."""
    model_path = os.path.join(output_dir, 'lstm_model.pth')
    torch.save({
        'model_state_dict': model.state_dict(),
        'config': config,
        'rmse': metrics['rmse'],
        'mae': metrics['mae']
    }, model_path)
    print(f"Model saved to {model_path}")


# ============== Main ==============
def main():
    print("=" * 60)
    print(f"LSTM Training: {SYMBOL}")
    print("=" * 60)
    print(f"Sequence Length: {SEQUENCE_LENGTH}")
    print(f"Prediction Horizon: {PREDICTION_HORIZON} day(s)")
    print(f"Use Returns: {USE_RETURNS}")
    print("-" * 60)
    
    # Load data
    script_dir = os.path.dirname(__file__)
    data_path = os.path.join(script_dir, DATA_FILE)
    
    if not os.path.exists(data_path):
        print(f"Data file not found: {data_path}")
        print("Run extract_data_from_api.py first.")
        return
    
    df = load_data(data_path, SYMBOL)
    print(f"Loaded {len(df)} records for {SYMBOL}")
    print(f"Date range: {df.index[0]} to {df.index[-1]}")
    
    # Prepare features
    data = prepare_features(df, USE_RETURNS)
    print(f"Data type: {'Daily Returns' if USE_RETURNS else 'Raw Prices'}")
    
    # Prepare train/test split
    X_train, y_train, X_test, y_test, scaler, split_idx = prepare_train_test_data(
        data, SEQUENCE_LENGTH, PREDICTION_HORIZON, TEST_SPLIT_RATIO, USE_RETURNS
    )
    
    print(f"Training samples: {len(X_train)}")
    print(f"Test samples: {len(X_test)}")
    print("-" * 60)
    
    # Build and train model
    model = LSTMModel(
        input_size=1,
        hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS,
        output_size=1
    )
    
    print("Training...")
    losses = train_model(model, X_train, y_train, EPOCHS, BATCH_SIZE, LEARNING_RATE)
    
    # Evaluate
    print("-" * 60)
    print("Evaluation:")
    metrics = evaluate_model(model, X_test, y_test, scaler, USE_RETURNS)
    
    print(f"RMSE: {metrics['rmse']:.6f}")
    print(f"MAE: {metrics['mae']:.6f}")
    if metrics['dir_acc'] is not None:
        print(f"Directional Accuracy: {metrics['dir_acc']:.1f}%")
    print(f"Naive Baseline RMSE: {metrics['naive_rmse']:.6f}")
    print(f"Beats Naive: {'Yes' if metrics['rmse'] < metrics['naive_rmse'] else 'No'}")
    
    # Plot and save
    plot_results(metrics, losses, SYMBOL, USE_RETURNS, script_dir)
    
    config = {
        'symbol': SYMBOL,
        'sequence_length': SEQUENCE_LENGTH,
        'prediction_horizon': PREDICTION_HORIZON,
        'use_returns': USE_RETURNS,
        'hidden_size': HIDDEN_SIZE,
        'num_layers': NUM_LAYERS
    }
    save_model(model, metrics, config, script_dir)
    
    print("=" * 60)
    print("Done!")


if __name__ == '__main__':
    main()
