##
# File:    MultiProcUtilTests.py
# Author:  jdw
# Date:    17-Nov-2018
#
# Updates:
#
##
"""

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"

import logging
import random
import re
import string
import unittest

from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

logging.basicConfig(level=logging.INFO, format=u'%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StringTests(object):
    """  A skeleton class that implements the interface expected by the multiprocessing
         utility module --

    """

    def __init__(self, **kwargs):
        pass

    def reverser(self, dataList, procName, optionsD, workingDir):
        """  Lexically reverse the characters in the input strings.
             Flag strings with numerals as errors.

             Read input list and perform require operation and return list of
                inputs with successful outcomes.
        """
        successList = []
        retList1 = []
        retList2 = []
        diagList = []
        for tS in dataList:
            if re.search('[8-9]', tS):
                continue
            rS1 = tS[::-1]
            rS2 = rS1 + tS
            diag = len(rS2)
            successList.append(tS)
            retList1.append(rS1)
            retList2.append(rS2)
            diagList.append(diag)

        logger.debug("%s dataList length %d successList length %d" % (procName, len(dataList), len(successList)))
        #
        return successList, retList1, retList2, diagList


class MultiProcUtilTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True

    def tearDown(self):
        pass

    def testMultiProcString(self):
        """

        """
        try:

            sCount = 10000
            sLength = 100
            dataList = []
            for ii in range(sCount):
                dataList.append('9' + ''.join(random.choice(string.ascii_uppercase) for _ in range(sLength)))
                dataList.append(''.join(random.choice(string.ascii_uppercase) for _ in range(sLength)))
            #
            logger.info("Length starting list is %d" % len(dataList))

            sTest = StringTests()

            mpu = MultiProcUtil(verbose=True)
            mpu.set(workerObj=sTest, workerMethod="reverser")

            ok, failList, resultList, diagList = mpu.runMulti(dataList=dataList, numProc=4, numResults=2, chunkSize=10)
            #
            logger.info("Multi-proc %r failures %r  result length %r %r" % (
                ok, len(failList), len(resultList[0]), len(resultList[1])))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteMultiProc():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MultiProcUtilTests("testMultiProcString"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite1 = suiteMultiProc()
        unittest.TextTestRunner(verbosity=2).run(mySuite1)
