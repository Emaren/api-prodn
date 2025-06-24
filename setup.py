"""Setup for AoE2 Betting Client and mgz_hd Replay Parser."""
from setuptools import setup, find_packages

setup(
    name='aoe2-betting-client',
    version='0.1.0',
    description='Parse Age of Empires 2 HD recorded games and integrate with a betting dapp.',
    url='https://github.com/Emaren/aoe2hd-parsing',
    license='MIT',
    author='Emaren',
    author_email='your-email@example.com',
    packages=find_packages(),
    install_requires=[
        'aocref>=2.0.20',
        'construct==2.8.16',
        'dataclasses==0.8; python_version < "3.7"',
        'tabulate>=0.9.0',
        'requests',
        'flask',
        'watchdog'
    ],
    entry_points={
        'console_scripts': [
            'mgz_hd=mgz_hd.cli:main',  # renamed to reflect your fork
            'aoe2client=client:process_replay',
            # 'aoe2watch=watch_replays:main',
        ],
    },
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ]
)
