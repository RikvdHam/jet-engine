#!/usr/bin/env python
import os
import re
 
import toml # type: ignore
import shutil # type: ignore
import fire # type: ignore

PRE_RELEASE_PLACEHOLDER = 'SNAPSHOT'
PRE_RELEASE_PATTERN = re.compile(fr'^\d+.\d+.\d+(-{PRE_RELEASE_PLACEHOLDER})$')
VERSION_FILEPATH = os.path.join('.', 'VERSION')
VERSION_PATTERN = re.compile(fr'^\d+.\d+.\d+(-{PRE_RELEASE_PLACEHOLDER})?$')
TOML_FILEPATH = os.path.join('.', 'pyproject.toml')
REPOSITORY_FOLDER = os.path.join('D:\\', 'Ph-Workstation-Shared', 'DA',
                                 'python_repository', 'repository')

def get(with_pre_release_placeholder: bool = False) -> str:
    with open(VERSION_FILEPATH, 'r') as version_file:
        version_lines = version_file.readlines()
        assert len(version_lines) == 1, 'Version file is malformed'
        version = version_lines[0]
        assert VERSION_PATTERN.match(version), 'Version string is malformed'
        if with_pre_release_placeholder:
            return version
        else:
            return version.replace(f'-{PRE_RELEASE_PLACEHOLDER}', '')

def is_pre_release() -> bool:
    version = get(True)
    return PRE_RELEASE_PATTERN.match(version)
        
def push_version(major: int, minor: int, patch: int) -> None:
    version = f'{major}.{minor}.{patch}'
    if is_pre_release():
        print("NOTE: Pushing pre-release version to VERSION file.")
        version = version + f'-{PRE_RELEASE_PLACEHOLDER}'
    with open(VERSION_FILEPATH, 'w') as version_file:
        version_file.write(version)
        
def patch():
    version = get()
    major, minor, patch = version.split('.')
    push_version(major, minor, int(patch) + 1)

def minor():
    version = get()
    major, minor, patch = version.split('.')
    push_version(major, int(minor) + 1, 0)

def major():
    version = get()
    major, minor, patch = version.split('.')
    push_version(int(major) + 1, 0, 0)
    
def get_package_name():
    with open(TOML_FILEPATH, 'r') as file:
        toml_file = toml.load(file)
        
    try: 
        package_name = toml_file['project']['name']
    except KeyError:
        raise KeyError('Package name not defined in pyproject.toml')

    return package_name

def push():
    package_name = get_package_name()
    
    dist_folder = os.path.abspath('dist')
    package_folder = os.path.join(REPOSITORY_FOLDER, package_name)
    os.makedirs(package_folder, exist_ok=True)
    
    if not os.path.exists(dist_folder):
        raise FileNotFoundError('Distribution package does not exists.')
    
    for filename in os.listdir(dist_folder):
        if filename.endswith(('.whl')):
            shutil.copy(os.path.join(dist_folder, filename), 
                        os.path.join(package_folder, filename))
            print(f'Pushed {filename} to repository {package_name}')
            
def remove_dist():
    dist_folder = os.path.abspath('dist')
    
    if not os.path.exists(dist_folder):
        raise FileNotFoundError('Distribution package does not exists.')
    
    shutil.rmtree(dist_folder)
    print(f'Removed dist folder {dist_folder}.')

if __name__ == "__main__":
    fire.Fire({
        'get': get,
        'patch': patch,
        'minor': minor,
        'major': major,
        'push': push,
        'remove-dist': remove_dist
    })