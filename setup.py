from setuptools import setup


setup(
    name='cldfbench_diidxaza',
    py_modules=['cldfbench_diidxaza'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'diidxaza=cldfbench_diidxaza:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
        'pyglottolog',
        'pydictionaria>=2.1',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
