module.exports = {
  apps: [
    {
      name: 'api-prodn',
      cwd: '/var/www/api-prodn',
      script: '/var/www/api-prodn/venv/bin/python',
      args: '-m uvicorn app:app --host 127.0.0.1 --port 3330',
      interpreter: 'none',
      instances: 1,
      exec_mode: 'fork',
      env: {
        NODE_ENV: 'production',
        ENV: 'production',
        PYTHONPATH: '/var/www/api-prodn',
        DOTENV_CONFIG_PATH: '/var/www/api-prodn/.env.production',
      },
      error_file: '/root/.pm2/logs/api-prodn-error.log',
      out_file:   '/root/.pm2/logs/api-prodn-out.log',
    },
  ],
};
