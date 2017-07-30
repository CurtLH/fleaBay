from setuptools import setup

requirements = [
    # package requirements go here
]

setup(
    name='fleabay',
    version='0.1.0',
    description="Data collection and ETL process",
    author="Curtis Hampton",
    author_email='CurtLHampton@gmail.com',
    url='https://github.com/CurtLH/fleabay',
    packages=['fleabay'],
    entry_points={
        'console_scripts': [
            'fleabay=fleabay.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='fleabay',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ]
)
