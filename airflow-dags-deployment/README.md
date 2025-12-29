# Airflow DAGs - Crypto OHLCV Silver Batch

## 📁 Project Structure

```
airflow-dags-deployment/
├── dags/
│   └── crypto_ohlcv_silver_batch_dag.py # Bronze → Silver OHLCV (ConfigMap)
│
└── README.md
```

## 🚀 DAG Overview

### Crypto OHLCV Silver Batch (`crypto-ohlcv-silver-batch`)

**Purpose**: Daily batch transformation of crypto trade data from Bronze to Silver layer with hourly OHLCV aggregations

**Workflow**:

```
Bronze Table (crypto_trades_raw) → Spark Aggregation → Silver Table (crypto_ohlcv_1h)
```

**Key Features**:

- ✅ Hourly OHLCV (Open, High, Low, Close, Volume) aggregations
- ✅ Volume-weighted average price (VWAP)
- ✅ Price change calculations
- ✅ Daily schedule (2 AM)
- ✅ ConfigMap-based deployment

**Tasks**:

1. `submit_crypto_silver_batch_job` - Apply SparkApplication from ConfigMap
2. `monitor_crypto_silver_batch_job` - Monitor SparkApplication execution
3. `cleanup_crypto_silver_batch_job` - Delete SparkApplication after completion

**Configuration**:

- **Schedule**: Daily at 2 AM (`0 2 * * *`)
- **Approach**: ConfigMap
- **Source**: Bronze table `crypto_trades_raw`
- **Target**: Silver table `crypto_ohlcv_1h`
- **Manifest**: `infra/k8s/compute/applications/spark/silver-layer/jobs/transform-crypto-silver-batch.yaml`

---

## 📦 Prerequisites

### 1. RBAC Configuration

Grant Airflow permission to submit and monitor Spark jobs:

```bash
cd infra/k8s/orchestration

# Apply ClusterRole (defines permissions)
kubectl apply -f rbac/spark-submit-clusterrole.yaml

# Apply ClusterRoleBinding (grants permissions to service accounts)
kubectl apply -f rbac/spark-submit-clusterrolebinding.yaml

# Verify RBAC resources
kubectl get clusterrole spark-submit-role
kubectl get clusterrolebinding spark-submit-binding
```

**Why RBAC is Required**:

- Airflow workers need permission to create and manage `SparkApplication` resources
- Workers need to monitor Spark driver/executor pods and retrieve logs
- Without RBAC, the DAG will fail with permission errors

For detailed RBAC configuration, see: [infra/k8s/orchestration/README.md](../infra/k8s/orchestration/README.md#rbac-configuration-for-airflow)

### 2. ConfigMap Creation

The DAG requires a ConfigMap containing the SparkApplication manifest:

```bash
cd infra/k8s/compute/scripts
./create_spark_manifests_configmap.sh
```

This script creates a ConfigMap named `spark-manifests` containing:

- `transform-crypto-silver-batch.yaml`: Silver transformation job manifest

**Verify ConfigMap**:

```bash
kubectl get configmap spark-manifests -n default
kubectl get configmap spark-manifests -n default -o yaml
```

---

## 🚀 Deployment

### Method 1: Manual Copy (Development)

```bash
# Deploy DAG
kubectl cp dags/crypto_ohlcv_silver_batch_dag.py airflow-worker-0:/opt/airflow/dags/ -n default

# Verify DAG is loaded
kubectl exec -it airflow-worker-0 -n default -- airflow dags list | grep crypto-ohlcv
```

### Method 2: Git-Sync (Production)

1. Create a private Git repository for DAGs
2. Add it to Airflow's Git-Sync configuration
3. Push DAG files to the repository

See [Git-Sync Setup](#-git-sync-setup-for-dag-deployment) below for detailed instructions.

---

## 🔄 Git-Sync Setup for DAG Deployment

### Overview

Instead of manually copying DAGs to Airflow pods, use Git-Sync to automatically sync DAGs from a private GitHub repository.

### Prerequisites

- Private GitHub repository for Airflow DAGs
- SSH access to GitHub
- Airflow deployed on Kubernetes with Helm

### Step 1: Create Private Repository

```bash
# Option 1: GitHub Web UI
# Visit: https://github.com/new
# Repository name: airflow-dags
# Visibility: Private

# Option 2: GitHub CLI
gh repo create airflow-dags --private
```

### Step 2: Push DAGs to Repository

```bash
cd airflow-dags-deployment

# Initialize git (if not already)
git init
git add dags/
git commit -m "Initial DAG: crypto_ohlcv_silver_batch"

# Add remote and push
git remote add origin git@github.com:<your-username>/airflow-dags.git
git branch -M main
git push -u origin main
```

### Step 3: Generate SSH Key

```bash
# Generate SSH key pair
ssh-keygen -t rsa -b 4096 -C "your_email@example.com" -f ~/.ssh/airflow-git-sync -N ""

# Output:
# Private key: ~/.ssh/airflow-git-sync
# Public key: ~/.ssh/airflow-git-sync.pub
```

### Step 4: Add Deploy Key to GitHub

```bash
# Copy public key
cat ~/.ssh/airflow-git-sync.pub

# Add to GitHub:
# 1. Go to: https://github.com/<your-username>/airflow-dags/settings/keys
# 2. Click "Add deploy key"
# 3. Title: "Airflow Git-Sync"
# 4. Paste public key
# 5. Click "Add key"
```

### Step 5: Convert Private Key to Base64

```bash
# Convert private key to base64
base64 ~/.ssh/airflow-git-sync -w 0 > /tmp/private-key-base64.txt

# Copy the base64 string
cat /tmp/private-key-base64.txt
```

### Step 6: Update Airflow Configuration

Edit `infra/k8s/orchestration/config/airflow.yaml`:

```yaml
dags:
  persistence:
    enabled: false # Disable persistence, use Git-Sync instead

  gitSync:
    enabled: true
    repo: git@github.com:<your-username>/airflow-dags.git # Your repository SSH URL
    branch: main
    rev: HEAD
    depth: 1
    maxFailures: 0
    subPath: "dags" # Subdirectory containing DAG files
    sshKeySecret: airflow-ssh-secret
    period: 60s # Sync interval
    wait: 60

# Create secret with SSH private key
extraSecrets:
  airflow-ssh-secret:
    data: |
      gitSshKey: '<paste-base64-private-key-here>'  # Paste base64 string from Step 5
```

### Step 7: Upgrade Airflow

```bash
cd infra/k8s/orchestration

# Upgrade Helm release
./scripts/install_airflow.sh

# Or manually:
helm upgrade openhouse-airflow helm/airflow \
  -n default \
  -f config/airflow.yaml \
  --timeout 10m
```

### Step 8: Verify Git-Sync

```bash
# Check secret created
kubectl get secret airflow-ssh-secret -n default

# View git-sync logs
SCHEDULER_POD=$(kubectl get pods -n default -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
kubectl logs $SCHEDULER_POD -n default -c git-sync --tail=30

# Check DAGs synced
kubectl exec -it $SCHEDULER_POD -n default -c scheduler -- \
  ls -la /opt/airflow/dags/repo/dags/

# Expected output:
# crypto_ohlcv_silver_batch_dag.py
```

### Successful Git-Sync Logs

```
INFO: syncing from "git@github.com:<your-username>/airflow-dags.git"
INFO: cloning into "/tmp/git"
INFO: synced 1 files from "origin/main"
```

### Benefits of Git-Sync

- ✅ Automatic DAG updates (no manual deployment)
- ✅ Version control for DAGs
- ✅ Secure SSH authentication
- ✅ No GitHub rate limits
- ✅ Sync every 60 seconds

---

## 📊 Monitoring

### Check DAG Status

```bash
# View DAG runs
kubectl exec -it airflow-scheduler-0 -n default -- airflow dags list-runs

# Check task instances
kubectl exec -it airflow-scheduler-0 -n default -- \
  airflow tasks states-for-dag-run crypto-ohlcv-silver-batch <run_id>
```

### Monitor Spark Jobs

```bash
# List SparkApplications
kubectl get sparkapplications

# View Spark driver logs
kubectl logs -l spark-role=driver,spark-app-name=transform-crypto-silver-batch -f

# Describe SparkApplication
kubectl describe sparkapplication transform-crypto-silver-batch
```

---

## 🔧 Troubleshooting

### DAG Not Appearing in Airflow UI

1. Check if DAG file is in the correct directory:

   ```bash
   kubectl exec -it airflow-worker-0 -n default -- ls /opt/airflow/dags/
   ```

2. Check DAG parsing errors:
   ```bash
   kubectl logs -f airflow-scheduler-0 -n default
   ```

### SparkApplication Submission Failed

1. Verify ConfigMap exists:

   ```bash
   kubectl get configmap spark-manifests -o yaml
   ```

2. Check if manifest is valid YAML:
   ```bash
   kubectl get configmap spark-manifests -o jsonpath='{.data.transform-crypto-silver-batch\.yaml}' | kubectl apply --dry-run=client -f -
   ```

### RBAC Permission Errors

```bash
# Check if ClusterRole exists
kubectl get clusterrole spark-submit-role

# Check if ClusterRoleBinding exists
kubectl get clusterrolebinding spark-submit-binding

# Verify service account has permissions
kubectl auth can-i create sparkapplications --as=system:serviceaccount:default:openhouse-spark-operator-spark
```

---

## 🔄 Updating the DAG

### Update ConfigMap

```bash
# 1. Edit manifest file
vim infra/k8s/compute/applications/spark/silver-layer/jobs/transform-crypto-silver-batch.yaml

# 2. Recreate ConfigMap
cd infra/k8s/compute/scripts
./create_spark_manifests_configmap.sh

# 3. Delete running job (if any) - new runs will use updated manifest
kubectl delete sparkapplication transform-crypto-silver-batch
```

### Update DAG Code

**Method 1: Manual Copy**

```bash
kubectl cp dags/crypto_ohlcv_silver_batch_dag.py airflow-worker-0:/opt/airflow/dags/
```

**Method 2: Git-Sync**

```bash
git add dags/crypto_ohlcv_silver_batch_dag.py
git commit -m "Update crypto OHLCV DAG"
git push origin main
# Wait 60 seconds for auto-sync
```

---

## � References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Spark Operator Documentation](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [Airflow Kubernetes Provider](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/index.html)
- [Spark Job Implementation](../spark-jobs/transform-crypto-silver-batch/README.md)
