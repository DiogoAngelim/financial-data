import os
import torch
import numpy as np
import pandas as pd
import torch.nn as nn
import torch.optim as optim
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# =========================
# ⚙️ Device Setup
# =========================
device = (
    torch.device("mps") if torch.backends.mps.is_available()
    else torch.device("cuda" if torch.cuda.is_available() else "cpu")
)
print(f"⚡ Using device: {device}")

# =========================
# 📦 Cache Store
# =========================
cache = {}

# =========================
# 📂 Load Assets
# =========================
def load_assets(exchange: str, symbols: list[str]):
    dfs = []
    for symbol in symbols:
        path = f"public/{exchange}/{symbol}.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file: {path}")

        df = pd.read_csv(path, parse_dates=["Date"])
        df = df[["Date", "Adj Close"]].rename(columns={"Adj Close": symbol})
        dfs.append(df)

    merged = dfs[0]
    for df in dfs[1:]:
        merged = pd.merge(merged, df, on="Date", how="inner")

    merged = merged.sort_values("Date").reset_index(drop=True)
    return merged

# =========================
# 📈 Preprocessing
# =========================
def compute_log_returns(df):
    prices = df.iloc[:, 1:].values
    log_returns = np.log(prices[1:] / prices[:-1])
    return log_returns.astype(np.float32)

# =========================
# 🧠 PPO Models
# =========================
class Actor(nn.Module):
    def __init__(self, n_assets):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_assets, 64),
            nn.ReLU(),
            nn.Linear(64, n_assets),
            nn.Softmax(dim=-1)
        )
    def forward(self, x): return self.net(x)

class Critic(nn.Module):
    def __init__(self, n_assets):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_assets, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )
    def forward(self, x): return self.net(x)

# =========================
# 🏆 Reward Function
# =========================
def reward_function(weights, returns, eps=1e-8):
    port_return = torch.sum(weights * returns)
    if returns.ndim == 1 or returns.shape[0] < 2:
        port_vol = torch.sqrt(torch.sum((weights * returns) ** 2) + eps)
    else:
        cov = torch.cov(returns.T)
        if cov.ndim == 0: cov = cov.unsqueeze(0).unsqueeze(0)
        elif cov.ndim == 1: cov = torch.diag(cov)
        w = weights.unsqueeze(0)
        port_vol = torch.sqrt(torch.matmul(w, torch.matmul(cov, w.T)) + eps).squeeze()
    return torch.clamp(port_return / (port_vol + eps), -5.0, 5.0)

# =========================
# 🔁 PPO Training
# =========================
def train_ppo(returns, n_assets, epochs=200, gamma=0.99, clip=0.2, lr=1e-3):
    actor, critic = Actor(n_assets).to(device), Critic(n_assets).to(device)
    opt_actor = optim.Adam(actor.parameters(), lr=lr)
    opt_critic = optim.Adam(critic.parameters(), lr=lr)

    states = torch.tensor(returns[:-1], dtype=torch.float32, device=device)
    next_states = torch.tensor(returns[1:], dtype=torch.float32, device=device)

    for epoch in range(epochs):
        weights = actor(states)
        rews = torch.stack([reward_function(w.detach(), r.detach()) for w, r in zip(weights, states)])
        vals = critic(states).squeeze()

        with torch.no_grad():
            adv = rews + gamma * critic(next_states).squeeze() - vals

        logp_old = torch.log(torch.sum(weights * states, dim=-1).clamp(min=1e-8))

        for _ in range(4):
            weights_new = actor(states)
            logp_new = torch.log(torch.sum(weights_new * states, dim=-1).clamp(min=1e-8))
            ratio = torch.exp(logp_new - logp_old.detach())
            surr1 = ratio * adv.detach()
            surr2 = torch.clamp(ratio, 1 - clip, 1 + clip) * adv.detach()
            actor_loss = -torch.min(surr1, surr2).mean()
            critic_loss = (critic(states).squeeze() - (rews.detach() + gamma * critic(next_states).squeeze().detach())).pow(2).mean()

            opt_actor.zero_grad(set_to_none=True)
            opt_critic.zero_grad(set_to_none=True)
            (actor_loss + 0.5 * critic_loss).backward()
            torch.nn.utils.clip_grad_norm_(actor.parameters(), 1.0)
            opt_actor.step()
            opt_critic.step()

    return actor

# =========================
# 🚀 Main Logic with Cache
# =========================
def get_optimal_weights(exchange, symbols):
    cache_key = f"{exchange}|{','.join(symbols)}"
    if cache_key in cache:
        print(f"🧠 Cache hit for {cache_key}")
        return cache[cache_key]

    print(f"🚀 Cache miss, training for {cache_key}")
    df = load_assets(exchange, symbols)
    log_returns = compute_log_returns(df)
    split = int(len(log_returns) * 0.8)
    train_returns = log_returns[:split]
    actor = train_ppo(train_returns, log_returns.shape[1])
    last_state = torch.tensor(train_returns[-1], dtype=torch.float32, device=device)
    weights = actor(last_state).detach().cpu().numpy().tolist()

    cache[cache_key] = weights
    return weights

# =========================
# 🌐 FastAPI
# =========================
class RequestModel(BaseModel):
    exchange: str
    symbols: list[str]

app = FastAPI(title="Optimal Portfolio API with Cache")

@app.post("/api/optimal-weights")
async def optimal_weights_api(data: RequestModel):
    try:
        weights = get_optimal_weights(data.exchange, data.symbols)
        return {"optimal_weights": weights, "cached": True if f"{data.exchange}|{','.join(data.symbols)}" in cache else False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# ▶️ Run
# =========================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
