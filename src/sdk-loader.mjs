/**
 * Resolves @earendil-works/pi-coding-agent and constructs an isolated ModelRuntime
 * directly from a Swarm job's `model` specification.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const CANDIDATE_SDK_PATHS = [
  process.env.PI_SDK_PATH,
  '/usr/local/lib/node_modules/@earendil-works/pi-coding-agent',
  '/opt/homebrew/lib/node_modules/@earendil-works/pi-coding-agent',
].filter(Boolean);

export async function loadPiSdk() {
  for (const dir of CANDIDATE_SDK_PATHS) {
    const entry = path.join(dir, 'dist/index.js');
    if (fs.existsSync(entry)) {
      const sdk = await import(pathToFileURL(entry).href);
      const req = createRequire(path.join(dir, 'package.json'));
      const { Type } = await import(pathToFileURL(req.resolve('typebox')).href);
      return { sdk, Type };
    }
  }
  const sdk = await import('@earendil-works/pi-coding-agent');
  const { Type } = await import('typebox');
  return { sdk, Type };
}

export async function createJobModelRuntime(sdk, modelSpec) {
  const { ModelRuntime } = sdk;
  if (modelSpec.modelsPath && modelSpec.authPath) {
    const runtime = await ModelRuntime.create({
      authPath: modelSpec.authPath,
      modelsPath: modelSpec.modelsPath,
    });
    const model = runtime.getModel(modelSpec.provider, modelSpec.id);
    if (!model) {
      throw new Error(`Model ${modelSpec.provider}/${modelSpec.id} not found in ${modelSpec.modelsPath}`);
    }
    return { modelRuntime: runtime, model, agentDir: path.dirname(modelSpec.modelsPath) };
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-model-'));
  const authPath = path.join(tmpDir, 'auth.json');
  const modelsPath = path.join(tmpDir, 'models.json');
  fs.writeFileSync(authPath, '{}', { mode: 0o600 });

  const providerId = modelSpec.provider || 'swarm-provider';
  const baseUrl =
    modelSpec.baseUrl ||
    (modelSpec.vertexProject
      ? `https://aiplatform.googleapis.com/v1/projects/${modelSpec.vertexProject}/locations/${modelSpec.vertexLocation || 'global'}/endpoints/openapi`
      : undefined);

  if (!baseUrl) {
    throw new Error(
      'model requires either `baseUrl`, `vertexProject`, or explicit `modelsPath` + `authPath`'
    );
  }

  const modelsDoc = {
    providers: {
      [providerId]: {
        name: modelSpec.providerName || providerId,
        baseUrl,
        api: modelSpec.api || 'openai-completions',
        apiKey: modelSpec.apiKey || 'gated-by-pi-swarm',
        authHeader: true,
        compat: {
          supportsStore: false,
          supportsDeveloperRole: false,
          supportsReasoningEffort: Boolean(modelSpec.reasoning),
          maxTokensField: 'max_tokens',
          ...(modelSpec.compat || {}),
        },
        models: [
          {
            id: modelSpec.id,
            name: modelSpec.name || modelSpec.id,
            reasoning: Boolean(modelSpec.reasoning),
            contextWindow: modelSpec.contextWindow || 1_000_000,
            maxTokens: modelSpec.maxTokens || 65_535,
            cost: modelSpec.cost || { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          },
        ],
      },
    },
  };

  fs.writeFileSync(modelsPath, JSON.stringify(modelsDoc, null, 2), { mode: 0o600 });
  const runtime = await ModelRuntime.create({ authPath, modelsPath });
  const model = runtime.getModel(providerId, modelSpec.id);
  if (!model) {
    throw new Error(`Failed to initialize model ${providerId}/${modelSpec.id}`);
  }
  return { modelRuntime: runtime, model, agentDir: path.join(tmpDir, 'agent') };
}
