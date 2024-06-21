import os

import lazyllm
from lazyllm import launchers


class TestFn_Launcher(object):
    
    def test_slurm(self):
        launcher = launchers.slurm(
            partition='pat_rd',
            nnode=1,
            nproc=1,
            ngpus=1,
            sync=False
            )
        assert launcher.partition == 'pat_rd'

    def test_empty(self):
        launcher = launchers.empty()
        assert launcher.subprocess == False

    def test_sco(self):
        launcher = launchers.sco(
            partition='pat_rd',
            nnode=1,
            nproc=1,
            ngpus=1,
            sync=False
        )
        assert launcher.partition == 'pat_rd'

    def test_remote(self):
        # empty launcher
        origin_launcher = lazyllm.config.impl['launcher']
        os.environ["LAZYLLM_DEFAULT_LAUNCHER"] ='empty'
        lazyllm.config.add('launcher', str, 'empty', 'DEFAULT_LAUNCHER')
        launcher = launchers.remote(
            sync=False
        )
        assert type(launcher) is launchers.empty
        assert launcher.sync == False
        os.environ["LAZYLLM_DEFAULT_LAUNCHER"]='slurm'
        lazyllm.config.add('launcher', str, 'empty', 'DEFAULT_LAUNCHER')
        launcher = launchers.remote(
            sync=False
        )
        assert type(launcher) is launchers.slurm
        assert launcher.sync == False
        os.environ["LAZYLLM_DEFAULT_LAUNCHER"]='sco'
        lazyllm.config.add('launcher', str, 'empty', 'DEFAULT_LAUNCHER')
        launcher = launchers.remote(
            sync=False
        )
        assert type(launcher) is launchers.sco
        assert launcher.sync == False

        os.environ["LAZYLLM_DEFAULT_LAUNCHER"] = origin_launcher
        lazyllm.config.add('launcher', str, 'empty', 'DEFAULT_LAUNCHER')
