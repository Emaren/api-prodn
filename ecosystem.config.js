module.exports = {
  apps: [
    {
      name: 'api-prodn',
      script: 'app.py',
      interpreter: 'python3',
      instances: 1,
      exec_mode: 'fork',
      env: {
        // keep the shared ones…
        NODE_ENV: 'production',
        // …but point to the prod-nightly env file
        DOTENV_CONFIG_PATH: '/var/www/api-prodn/.env.production',
      },
      // optional: different log files
      error_file: '/root/.pm2/logs/api-prodn-error.log',
      out_file:   '/root/.pm2/logs/api-prodn-out.log',
    },
  ],
};
