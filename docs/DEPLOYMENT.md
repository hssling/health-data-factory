# Deployment Playbook

## CI/CD flows
- `ci.yml`: static checks and tests.
- `nightly-build.yml`: scheduled dataset build and artifact publication.
  - Optional publish to Hugging Face and Kaggle.
- `continuous-ingestion.yml`: hourly continuous run for registry-enabled datasets.
  - Auto-publishes TB datasets to HF/Kaggle when credentials are configured.
- `deploy.yml`: container image build/push to GHCR, optional webhook trigger.
  - Trigger manually via Actions `workflow_dispatch`.
- `publish-platforms.yml`: manual platform publication workflow (HF/Kaggle).

## GitHub configuration
1. Ensure repository Actions are enabled.
2. Add repository variable (optional default):
   - `ENABLE_IMAGE_PUSH=true` to enable image pushes and webhook deployment.
3. Add secrets (when pushing/deploying):
   - `GHCR_TOKEN` (recommended): PAT with `write:packages` scope.
   - `DEPLOY_WEBHOOK_URL` (optional): endpoint used to trigger your runtime platform deploy.
   - `HF_TOKEN` (for Hugging Face uploads)
   - `KAGGLE_USERNAME`, `KAGGLE_KEY` (for Kaggle uploads)
4. Confirm `GITHUB_TOKEN` has package write permissions (workflow already requests this).
5. Add variables:
   - `HDB_HF_DATASET_REPO_ID` (e.g. `org/health-data-factory-datasets`)
   - `HDB_HF_MODEL_REPO_ID` (e.g. `org/health-data-factory-models`)
   - `HDB_KAGGLE_DATASET_SLUG` (e.g. `username/health-data-factory-datasets`)
   - `HDB_KAGGLE_MODEL_SLUG` (e.g. `username/health-data-factory-models`)

## Running deploy
1. Open GitHub Actions -> `deploy`.
2. Click `Run workflow`.
3. Set `push_images=true` when you want GHCR pushes.

## Running platform publication
1. Open GitHub Actions -> `publish-platforms`.
2. Click `Run workflow`.
3. Choose `dataset_id` and target (`hf`, `kaggle`, or `all`).

## Container images
`deploy.yml` publishes:
- `ghcr.io/<owner>/<repo>/api:latest`
- `ghcr.io/<owner>/<repo>/api:<sha>`
- `ghcr.io/<owner>/<repo>/worker:latest`
- `ghcr.io/<owner>/<repo>/worker:<sha>`

## Example runtime deployment
Use your platform's container deployment with:
- API image command:
  `uv run uvicorn apps.api.main:app --host 0.0.0.0 --port 8000`
- Worker image command:
  `uv run hdb run demo_dataset --full-refresh`

## DVC remote setup
Default local remote exists in `.dvc/config`.

For S3:
```bash
uv run dvc remote modify localremote url s3://my-bucket/health-data-factory
uv run dvc remote modify localremote endpointurl https://s3.amazonaws.com
```

For R2:
```bash
uv run dvc remote modify localremote url s3://my-r2-bucket/health-data-factory
uv run dvc remote modify localremote endpointurl https://<accountid>.r2.cloudflarestorage.com
```

Note: Kaggle publication uploads both dataset artifacts and model artifacts as separate Kaggle datasets.
