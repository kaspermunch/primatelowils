
from gwf import Workflow, AnonymousTarget
from gwf.workflow import collect

import glob, os, re, gc

gwf = Workflow(defaults=dict(account='Primategenomes'))

def modpath(p, parent=None, base=None, suffix=None):
    par, name = os.path.split(p)
    name_no_suffix, suf = os.path.splitext(name)
    if type(suffix) is str:
        suf = suffix
    if parent is not None:
        par = parent
    if base is not None:
        name_no_suffix = base
    new_path = os.path.join(par, name_no_suffix + suf)
    if type(suffix) is tuple:
        assert len(suffix) == 2
        new_path, nsubs = re.subn(r'{}$'.format(suffix[0]), suffix[1], new_path)
        assert nsubs == 1, nsubs
    return new_path

def write_segment_files(input_path):
    segment_path = modpath(input_path, parent=segment_dir, suffix='.h5')
    inputs = {
        'path': input_path
        }
    outputs = {
        'path': segment_path,
    }
    options = {'memory': '50g',
               'walltime': '00:15:00'}
    spec = f"""

    python scripts/write_segment_files.py {input_path} {segment_path} 
    
    """
    return AnonymousTarget(inputs=inputs, outputs=outputs, options=options, spec=spec)


def write_window_files(input_path):
    windows_path = modpath(input_path, parent=windows_dir, suffix='.h5')  
    inputs = {
        'path': input_path
        }
    outputs = {
        'path': windows_path,
    }
    options = {'memory': '8g',
               'walltime': '00:15:00'}
    spec = f"""

    python scripts/write_window_files.py {input_path} {windows_path}
    
    """
    return AnonymousTarget(inputs=inputs, outputs=outputs, options=options, spec=spec)


def write_lowils_region_files(input_path, cutoff_fraction=0.1):
    lowils_path = modpath(input_path, parent=lowils_dir, suffix=f'_{int(cutoff_fraction*100)}.h5')  
    inputs = {
        'input_path': input_path
        }
    outputs = {
        'lowils_path': lowils_path,
    }
    options = {'memory': '8g',
               'walltime': '00:10:00'}
    spec = f"""

    python scripts/write_lowils_region_files.py {input_path} {lowils_path} {cutoff_fraction}
    
    """
    return AnonymousTarget(inputs=inputs, outputs=outputs, options=options, spec=spec)

postprop_file_names = glob.glob('/home/kmt/Primategenomes/data/final_tables_unclock/*.HDF')

segment_dir = '/home/kmt/Primategenomes/people/kmt/low_ils_regions/steps/segments'
windows_dir = '/home/kmt/Primategenomes/people/kmt/low_ils_regions/steps/windows'  
lowils_dir = '/home/kmt/Primategenomes/people/kmt/low_ils_regions/steps/low_ils_regions'  


if not os.path.exists(segment_dir):
    os.makedirs(segment_dir)
sgm_tasks = gwf.map(write_segment_files, postprop_file_names)

if not os.path.exists(windows_dir):
    os.makedirs(windows_dir)
collected = collect(sgm_tasks.outputs, ['path'])
win_tasks = gwf.map(write_window_files, collected['paths'])
    
if not os.path.exists(lowils_dir):
    os.makedirs(lowils_dir)    
collected = collect(win_tasks.outputs, ['path'])
max_ils_percent = 0
reg_tasks = gwf.map(write_lowils_region_files, collected['paths'], 
    extra=dict(cutoff_fraction=max_ils_percent/100), name=f'write_lowils_region_files_{max_ils_percent}')
max_ils_percent = 5
reg_tasks = gwf.map(write_lowils_region_files, collected['paths'], 
    extra=dict(cutoff_fraction=max_ils_percent/100), name=f'write_lowils_region_files_{max_ils_percent}')
max_ils_percent = 10
reg_tasks = gwf.map(write_lowils_region_files, collected['paths'], 
    extra=dict(cutoff_fraction=max_ils_percent/100), name=f'write_lowils_region_files_{max_ils_percent}')

