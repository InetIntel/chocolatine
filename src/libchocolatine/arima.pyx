#!/usr/bin/python3
# cython: language_level=3

# This file is part of chocolatine
# 
# Copyright (C) 2021 The Regents of the University of California
# All Rights Reserved
#
# Permission to copy, modify, and distribute this software and its
# documentation for academic research and education purposes, without fee, and
# without a written agreement is hereby granted, provided that
# the above copyright notice, this paragraph and the following paragraphs
# appear in all copies.
#
# Permission to make use of this software for other than academic research and
# education purposes may be obtained by contacting:
#
# Office of Innovation and Commercialization
# 9500 Gilman Drive, Mail Code 0910
# University of California
# La Jolla, CA 92093-0910
# (858) 534-5815
# invent@ucsd.edu
#
# This software program and documentation are copyrighted by The Regents of the
# University of California. The software program and documentation are supplied
# "as is", without any accompanying services from The Regents. The Regents does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO
# OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.
#
# ---------------------
#
# The source code in this file is a modified version of code that was
# originally written by Andreas Guillot, and was released under the
# BSD 2-Clause license.
#
# The terms of the original license are below:
#
# Copyright (c) 2021, Andréas Guillot
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
####
#
# Portions of this source code are Copyright (c) 2023 Georgia Tech Research
# Corporation. All Rights Reserved. Permission to copy, modify, and distribute
# this software and its documentation for academic research and education
# purposes, without fee, and without a written agreement is hereby granted,
# provided that the above copyright notice, this paragraph and the following
# three paragraphs appear in all copies. Permission to make use of this
# software for other than academic research and education purposes may be
# obtained by contacting:
#
#  Office of Technology Licensing
#  Georgia Institute of Technology
#  926 Dalney Street, NW
#  Atlanta, GA 30318
#  404.385.8066
#  techlicensing@gtrc.gatech.edu
#
# This software program and documentation are copyrighted by Georgia Tech
# Research Corporation (GTRC). The software program and documentation are
# supplied "as is", without any accompanying services from GTRC. GTRC does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL GEORGIA TECH RESEARCH CORPORATION BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF GEORGIA TECH RESEARCH CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE. GEORGIA TECH RESEARCH CORPORATION SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND  GEORGIA TECH RESEARCH CORPORATION HAS
# NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.
###############################################################################
# Imports

# Needed to make the same work for both python3.6 and python3.7
from __future__ import generator_stop

# Plotting
#from plot import Plot

# Data Formatting
import pandas as pd
import numpy as np

# Statistical analysis
from statsmodels.tsa.stattools import adfuller  # Check stationarity
from statsmodels.tsa.arima.model import ARIMA
import statsmodels.tools.sm_exceptions

# ARIMA error
from numpy.linalg import LinAlgError

# Error estimation
from sklearn.metrics import mean_squared_error
from math import sqrt

# Multiprocessing
import multiprocessing as mp

# Type hinting
from typing import List, Tuple, Iterable, Optional, Dict

# Utility
import sys
import logging
import warnings
import queue as Queue
from timeit import default_timer as timer
from math import ceil, floor
import os

###############################################################################


def queue_iterator(queue) -> Iterable:
    """
    Return an element of 'queue'. If 'queue' is empty, stop
    """
    while True:
        try:
            yield queue.get(False)
        except Queue.Empty:
            return


class Arima():
    """
    Performs ARIMA on different datasets to detect outages.

    ARIMA is used to be able to model a signal. Once the signal has been
    modelled, it is possible to look for values that are abnormally low to
    outages. 'analyze_signal' is the only function that should be called from
    outside.
    """

    def __init__(self, test_time, n, difference_order, arima_order,
                 save, plot, signal_number, thresholds):
        """
        Create an ARIMA object.

        Parameters
        ----------
        test_time: List[pd.Timestamp]
            Two elements: beginning and end of data
        n: int
            The number of predictions that are computed at each step
        difference_order: List[int]
            Parameters that are required to difference the time series.
        arima_order: int
            Maximum number of ARMA parameters so that (p + q < 'arima_order')
        save: bool
            Variable specifying if pickle files of the datasets should be
            created or not.
        plot: bool
            Plot the data or not.
        signal_number: int
            A simple way to ensure that multiple sections will have different
            figure names.
        thresholds: List
            A list of numbers indicating the thresholds for numbers
        """

        # Remove statsmodels warnings
        warnings.filterwarnings("ignore")

        # Delimiting the different test periods
        self.training_time, self.validation_time, self.test_time = (
            self.get_time_splits(test_time)
        )

        self.n = n
        self.difference_order = difference_order
        self.arima_order = arima_order
        self.save = save
        self.plot = plot
        self.signal_number = signal_number
        self.thresholds = thresholds

        self.pkl_dir = 'pickles/'
        self.results_dir = 'results/'

        # Setup logging parameters
        logging.basicConfig(filename='logs/logs.log', level=logging.DEBUG,
                            format='%(asctime)s %(processName)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

        logging.info('Creating an ARIMA object with the following parameters: '
                     'n = {}, difference_order = {}, arima_order = {}.'
                     ''.format(self.n, self.difference_order, self.arima_order)
                     )
        logging.info('training_time: {}'.format(self.training_time))
        logging.info('validation_time: {}'.format(self.validation_time))
        logging.info('test_time: {}'.format(self.test_time))

    def difference(self, df: pd.Series, difference_order) -> pd.Series:
        """
        Difference df using 'difference_order'.

        If multiple parameters are inside 'difference_order', apply each
        of these differences. The resulting dataframe has max(difference_order)
        less elements since differencing removes some data. This is why padding
        is added at the beginning of the dataframe.

        Parameters
        ----------
        df: pd.Series
            Dataframe that contains the period of interest and some padding.

        Returns
        -------
        pd.Series
            The differenced dataframe with no padding left.
        """

        res = df.copy()
        res.name = 'diff-' + df.name

        if isinstance(difference_order, list):
            for lag in reversed(sorted(difference_order)):
                res = pd.Series(
                    res[lag:].values - res[:-lag].values,
                    index=res[lag:].index,
                    name=res.name
                )
        else:
            res = res.diff(periods=difference_order).iloc[
                difference_order:]

        return res

    def invert_diff(self, prediction: float, history: pd.Series) -> float:
        """
        Invert the difference of a single value.

        The differenced 'prediction' is inverted to correspond to real data by
        adding the numbers that have been subtracted while calling difference.

        Parameters
        ----------
        prediction: float
            The value predicted by the ARMA model.

        Returns
        -------
        float
            The value brought back to its real scale.
        """

        res = prediction

        if len(self.difference_order) > 1:
            if self.difference_order != [1, 2016]:
                raise NotImplementedError

            d2016 = self.difference(history, 2016)

            res += d2016.iloc[-1]
            res += history.iloc[-2016]
        else:
            res += history[-self.difference_order[0]]

        return res

    def test_stationarity(self, df: pd.Series) -> int:
        """
        Perform the Dickey-Fuller test to determine if the timeseries is
        stationary or not.

        Returns
        -------
        int
            0 if not stationary, 1: 99%, 5: 95%, 10: 90%
        """

        # Series is constant, adfuller() isn't going to work but we should
        # be able to assume stationarity, right?
        if df.min() == df.max():
            return 1

        logging.debug("Time series {} of length {} passed to adfuller".format(
                      df.name, len(df)))

        # Perform Dickey-Fuller test
        dftest = adfuller(df)

        # Store the output into a pandas series
        dfoutput = pd.Series(
            dftest[0:4],
            index=['TestStatistic', 'p-value', '# Lags Used',
                   'Number of Observations Used']
        )

        for key, value in dftest[4].items():
            dfoutput['Critical Value (%s)' % key] = value

        # Analyze the results
        if dfoutput['TestStatistic'] < dfoutput['Critical Value (1%)']:
            res = 1
        elif dfoutput['TestStatistic'] < dfoutput['Critical Value (5%)']:
            res = 5
        elif dfoutput['TestStatistic'] < dfoutput['Critical Value (10%)']:
            res = 10
        else:
            res = 0

        return res

    def outage_detection(self, real: float, predicted: float,
                         lower: float) -> float:
        """
        Compares real and predicted values to determine if an outage is
        occurring.

        The real value is compared to the one obtained by the ARMA model.
        'lower' is the lower bound of the confidence interval associated with
        the prediction.

        Parameters
        ----------
        real: float
            The differenced value from the original signal.
        predicted: float
            The differenced value that has been predicted by the ARMA model.
        lower: float
            Lower bound of the confidence interval.
        higher: bool
            Value specifying if the current differenced prediction is above the
            current differenced real value.

        Returns
        -------
        Tuple[bool, float]
            the boolean describes if it should be considered as an outage or
            not, and the float value is the distance between the point and the
            confidence interval.
        """

        return (predicted - real) / (predicted - lower)

    def save_forecasts(self, potential_outages, test, diff_test, predictions,
                       std, stds) -> None:
        """
        Save the different structures created in walk_forward_validation.
        """

        prefix = '{}{}-{}_{}-{}_'.format(
            self.pkl_dir, self.signal_number, test.name,
            test.index[0].timestamp(), test.index[-1].timestamp()
        )

        potential_outages.to_pickle(prefix + 'potential_outages.pkl')
        test.to_pickle(prefix + 'test.pkl')
        diff_test.to_pickle(prefix + 'diff_.pkl')
        np.save(prefix + 'predictions', predictions)
        np.save(prefix + 'std', std)
        np.save(prefix + 'stds', stds)

    def plot_forecasts(self, df, order, predictions, std, potential_outages
                       ) -> None:

        return
        #p = Plot()

        title = '{}Predictions vs {}data (order = {})'.format(
            '' if 'diff' in df.name else 'Inverted ',
            'differenced ' if 'diff' in df.name else '',
            order
        )

        p.plot_predictions(
            df.index,
            df.fillna(0).values,
            df.iloc[-len(predictions):].index,
            predictions,
            std,
            potential_outages,
            self.outages,
            title,
            '{}-{}_{}-{}'.format(self.signal_number, df.name,
                                 df.index[-len(predictions)].timestamp(),
                                 df.index[-1].timestamp()
                                 ),
            self.training_time[1],
            self.validation_time[1]
        )

    def plot_signal(self, df):
        filename = '{}-{}_{}-{}.pdf'.format(self.signal_number, df.name,
                                            df.index[0].timestamp(),
                                            df.index[-1].timestamp()
                                            )
        return
        # p = Plot()

        p.plot_number_of_unique_ip(
            df, 'ip_' + filename, title='Number of unique IP addresses for {} '
            'between {} and {}'.format(df.name, df.index[0].timestamp(),
                                       df.index[-1].timestamp())
        )
        p.plot_autocorrelation(
            df, 'acf_' + filename, title='Autocorrelation for {} between {} an'
            'd {}'.format(df.name, df.index[0].timestamp(),
                          df.index[-1].timestamp())
        )
        p.plot_partial_autocorrelation(
            df, 'pacf_' + filename, title='Partial autocorrelation for {} betw'
            'een {} and {}'.format(df.name, df.index[0].timestamp(),
                                   df.index[-1].timestamp())
        )

    def get_median_absolute_deviations(self, df: pd.DataFrame,
                                       predictions: List) -> List[float]:
        """
        Returns the confidence intervals that corresponds to the real data.

        Parameters
        ----------
        df: pd.DataFrame
            Contains the real data.

        predictions: List
            Contains the predictions for each value in df

        Returns
        -------
        List
            A list of 'self.n' elements that indicate the size of the
            confidence interval for each element
        """

        mads = []  # type: List[float]

        for n in range(self.n):
            a = predictions[n::self.n] - df[n::self.n]
            mads.append(a.abs().median() * 1.4826 * 3)
# XXX Alternate approach for eliminating outliers is to find the
# difference between 50th and 1st percentile.  Still has problems, but
# different problems.
#            a.median() - a.quantile(.01)

        return mads

    def get_current_value(self, value: float, history: pd.Series) -> float:
        """
        Get the current value by using the history with potentially inpainted
        values.

        Returns
        -------
        float
            A value differenced according to self.difference_order
        """

        res = value

        if len(self.difference_order) > 1:
            if self.difference_order != [1, 2016]:
                raise NotImplementedError

            res -= history.iloc[-2016]  # D2016
            res -= history.iloc[-1] - history.iloc[-2017]  # D1-2016
        else:
            res -= history.iloc[-self.difference_order[0]]

        return res

    def walk_forward_validation(self, df: pd.Series, diff_df: pd.Series,
                                order: Tuple[int, int, int], test_start:
                                pd.Timestamp, queue, mad, mode: str, ppw
                                ) -> Optional[pd.Series]:
        """
        Core method that trains ARMA models and computes the difference between
        the real values and the predictions.

        'order' is used to create an ARMA model that will compute predictions
        and look at how far from reality they are. 'test_start' is used to
        delimit the part that will be fed to the ARMA model as training and the
        part that is going to be used to test the model. 'queue' is used to
        send results if this function is multiprocessed.

        Parameters
        ----------
        df: pd.Series
            The real data.
        diff_df: pd.Series
            'df' differenced with 'self.difference_order'.
        order: Tuple[int, int]
            the (p, q) parameters for the ARMA model.
        test_start: pd.Timestamp
            The limit after which the evaluation process starts.
        queue:
            An optional queue that will be filled if the environment is
            multiprocessed. Is the difference between training and test since
            the queues are also used for the training part.
        mad:
            Contains the std values that have been computed during the training
            phase if the current phase is the test one.
        mode: str
            Mode will determine the behaviour of this fonction. 'validation'
            will insert the found error and the size of the prediction
            intervals inside the queue, 'test' will return the list of
            potential outages that were found.

        Returns
        -------
        Optional[pd.Series]
            Returns the time series of potential outages and None if the model
            did not converge.
        """

        start = timer()
        name = df.name

        # Values that are going to be fed to the ARIMA model. It is equal
        # to the training set at first but will gain values since it is
        # a rolling forecast
        history = df[:test_start].copy().drop(test_start, errors='ignore')
        diff_history = diff_df[:test_start].copy().drop(test_start,
                                                        errors='ignore')
        assert len(history) >= self.difference_order[0]
        assert len(diff_history) >= self.difference_order[0]

        # List containing the predictions for each value. Is later going to
        # be compared to the real values to compute the MSE
        predictions = []
        diff_predictions = []
        diff_std = []

        # Pandas series of the list of potential outages.
        potential_outages = pd.Series(name='t', dtype='float64')

        try:
            # For each value of the test set
            index = 0

            size = len(df[test_start:])

            while index <= size + self.n:
                # Case where the live mode will send and receive data
                # >= because index += n
                if index >= size:
                    if mode != 'live':
                        break

                    # Send t data to the main process
                    queue[1].put(potential_outages)
                    logging.debug('{} sent data'.format(name))

                    # Reset data that we want to send to the controller
                    potential_outages = pd.Series(name='t', dtype='float64')

                    # Receive new data from the main process
                    new_data = queue[0].get()

                    if new_data is None:
                        logging.debug('{} received none. Ending analysis.'
                                      ''.format(name))
                        queue[1].put(None)
                        return None

                    logging.debug('{} received new data'.format(name))

                    size += len(new_data)
                    df = pd.concat([df, new_data])

                # Feed the data to the model
                model = ARIMA(diff_history.values, order=order)

                # Fit the model
                try:
                    fitted_model = model.fit()
                except statsmodels.tools.sm_exceptions.MissingDataError as err:
                    logging.info('Error fitting model: {}'.format(err))
                    logging.debug('Input data: {}'.format(diff_history))
                    sys.exit(-1)
                except(ValueError):
                    logging.debug('Order {} for {} does not have stationary '
                                  'parameters'.format(order, name))
                    return None

                # Make a prediction
                forecast = fitted_model.get_forecast(steps=self.n)

                # Dont make too many predictions
                if index + self.n < len(df[test_start:]):
                    repeats = self.n
                else:
                    repeats = len(df[test_start:]) - index

                for i in range(repeats):
                    prediction = forecast.predicted_mean[i]
                    inverted_prediction = self.invert_diff(
                        prediction,
                        history
                    )
                    current_value = df[test_start:].iloc[index + i]
                    current_time = df[test_start:].index[index + i]

                    # Appending predictions
                    predictions.append(inverted_prediction)
                    diff_predictions.append(prediction)
                    diff_std.append(forecast.se_mean[i])

                    # Missing values
                    if np.isnan(current_value):
                        # Inpaint
                        history.at[current_time] = inverted_prediction
                        diff_history.at[current_time] = prediction
                    elif mode != 'validation':
                        # See if possible outage
                        t = self.outage_detection(
                            current_value,
                            inverted_prediction,
                            inverted_prediction - mad[(index + i) % len(mad)]
                        )

                        # Record the relative distance
                        potential_outages.at[current_time] = t

                        # Record the distance between prediction and real value
                        if abs(t) > 1:
                            # Inpaint
                            history.at[current_time] = inverted_prediction
                            diff_history.at[current_time] = prediction
                        else:
                            history.at[current_time] = current_value
                            diff_history.at[current_time] = (
                                self.get_current_value(current_value, history)
                            )
                    # Regular case for validation mode
                    else:
                        history.at[current_time] = current_value
                        diff_history.at[current_time] = (
                            self.get_current_value(current_value, history)
                        )

                    # Remove first element of history to keep a constant window
                    # size
                    if len(history) > ppw:
                        history = history[-ppw:]
                        diff_history = diff_history[-ppw:]

                index += self.n


            if mode == 'validation':
                # Computing the RMSE
                d = diff_df[test_start:].copy().to_frame()
                d['diff-predictions'] = diff_predictions
                d.dropna(inplace=True)
               
                fname = self.results_dir + "/" + df.name + "-" + \
                        "%d-%d-%d" % (order[0], order[1], order[2]) + \
                        ".predictions"
                with open(fname, "w") as f:
                    for i in range(0, len(predictions)):
                        current_value = df[test_start:].iloc[i]
                        current_time = df[test_start:].index[i]
                        f.write("%s,%.3f,%.3f\n" % (current_time,current_value,predictions[i]))

                error = sqrt(
                    mean_squared_error(
                        d[d.columns[0]].values,
                        d[d.columns[1]].values
                    )
                )

                # Getting the std for real values
                mad = self.get_median_absolute_deviations(
                    df[test_start:], predictions
                )

                # Inserting everything inside the queue
                queue.put((order, error, mad))

            elif mode == 'test':
                # Ongoing analysis wants predictions and inpainted values.
                if queue:
                    queue.put((history[test_start:],
                               diff_history[test_start:],
                               predictions))

                # Record data and plot figures
                if self.save:
                    resized_stds = (
                        mad * ceil(len(df[test_start:]) / len(mad))
                    )[:len(df[test_start:])]
                    self.save_forecasts(potential_outages, df[test_start:],
                                        diff_df[test_start:], diff_predictions,
                                        diff_std, resized_stds)
                if self.plot:
                    resized_stds = (
                        mad * ceil(len(df[test_start:]) / len(mad))
                    )[:len(df[test_start:])]

                    self.plot_forecasts(
                        df[self.training_time[0]:].interpolate(), order,
                        predictions, resized_stds, potential_outages
                    )

        # If the linear equations did not converge
        except(LinAlgError):
            logging.error('{} error: model did not converge.'.format(df.name))
            return None

        end = timer()
        logging.debug('Computing ARMA{} for {} took {:.2f} seconds.'.format(
                      order, df.name, end - start))

        # The validation mode does not use the return value.
        return potential_outages

    def find_best_model(self, df: pd.Series, diff_df: pd.Series, ppw,
                        multiprocessing=True) -> Tuple[Tuple[int, int],
                                                       List[float]]:
        """
        Tests a range of parameters and returns the best model.

        Different ARMA models are computed and evaluated using RMSE. The model
        with the lowest error, i.e. the lowest one, is then returned. 'diff_df'
        is 'df' differenced with 'self.difference_order'.

        Returns
        -------
        Tuple[int, int]
            The tuple containing the best (p, q) parameters.
        """

        logging.debug('Started training for "{}":'.format(df.name))

        # Results dataframe that will contain the performances for every
        # combination of (p, q)
        results = pd.DataFrame(columns=range(self.arima_order),
                               index=range(self.arima_order))

        # Queue that will contain the end results
        if multiprocessing:
            # Create a pool of n workers, where n is the number of cpus
            pool = mp.Pool(8)
            queue = mp.Manager().Queue()
        else:
            queue = mp.Queue()

        # Iterate over every parameter.
        for (p, q) in [(p, q) for p in range(self.arima_order) for q in
                       range(self.arima_order) if (p + q < self.arima_order and
                                                   p + q > 0)]:
            order = (p, 0, q)

            if multiprocessing:
                # Asynchronously start processes
                pool.apply_async(
                    self.walk_forward_validation,
                    args=(
                        df[:self.validation_time[1]],
                        diff_df[:self.validation_time[1]],
                        order,
                        self.validation_time[0],
                        queue,
                        None,
                        'validation',
                        ppw
                    )
                )
            else:
                self.walk_forward_validation(
                        df[:self.validation_time[1]],
                        diff_df[:self.validation_time[1]],
                        order, self.validation_time[0],
                        queue, None, 'validation', ppw)

            """
                fastarima.fast_walk_forward_validation(
                        df[:self.validation_time[1]],
                        diff_df[:self.validation_time[1]],
                        order[0], order[2], self.validation_time[0],
                        None, ppw)
            """
            """  # Non-parallelized (debug)
            self.walk_forward_validation(
                df[:self.validation_time[1]],
                diff_df[:self.validation_time[1]],
                order, self.validation_time[0], queue, None, 'validation'
            )
            """

        if multiprocessing:
            # End of the parallelizable part
            pool.close()
            pool.join()

        # Returned values with the information about the best configurations
        best_score = float('Inf')
        best_config = (-1, 0, -1)
        mads = []  # type: List[float]

        # Get the results from the pool
        for order, error, mad in queue_iterator(queue):

            logging.debug('{}\t{}: {:.3f} mad: {}'.format(df.name, order, error, mad))

            # Save the result
            results.at[order[0], order[2]] = error  
            if error < best_score:
                best_score = error
                best_config = order
                mads = mad

        if self.save:
            if isinstance(self.difference_order, list):
                d = '-'.join([str(x) for x in self.difference_order])
            else:
                d = self.difference_order

            filename = self.results_dir + '{}_{}-{}_{}_{}_{}.csv'.format(
                df.name, df.index[0].timestamp(), df.index[-1].timestamp(),
                self.n, d, self.arima_order
            )
            results.to_csv(filename)
            logging.debug('Results saved in file: "{}"'.format(filename))

        logging.info('Best model for {} was {}, with error of {:.3f}.'.format(
                     df.name, best_config, best_score))

        return best_config, mads

    def get_time_splits(self, test_time: List[str]) -> Tuple[List[
            pd.Timestamp], List[pd.Timestamp], List[pd.Timestamp]]:
        """
        Returned the correct time periods.

        Takes 'test_time', converts it to pd.Timestamp, and separate it into
        three different parts: the training set, the validation set, and the
        test set extended with another week.

        Parameters
        ----------
        test_time: List[str]
            Contains two timestamps with beginnning and end of the test time
            period.

        Returns
        -------
        Tuple[List[pd.Timestamp], List[pd.Timestamp], List[pd.Timestamp]
            Three lists containing three time intervals.
        """

        test_time = list(map(pd.to_datetime, test_time))

        validation_time = [
            test_time[0] - pd.DateOffset(weeks=1),
            test_time[0] - pd.DateOffset(minutes=5)
        ]
        training_time = [
            validation_time[0] - pd.DateOffset(weeks=2),
            validation_time[0] - pd.DateOffset(minutes=5)
        ]

        return training_time, validation_time, test_time

    def test_signal(self, df: pd.Series) -> None:
        """
        Train different ARIMA models and returns the best one.
        """

        # Test stationarity
        error = self.test_stationarity(
            df[self.training_time[0]:self.training_time[1]]
        )
        if not error:
            logging.warning('The time series "{}" is *NOT* stationary.'
                            ''.format(df.name))
        else:
            logging.debug('The time series "{}" has a {}% chance to be '
                          'stationary.'.format(df.name, 100 - error))

    def sanitize_training(self, df: pd.Series, validation: pd.Series,
                          ppw, weeks=15) -> pd.Series:
        """
        Using 'weeks' weeks of data to sanitize the training set.

        The training set is sanitized using the median of the 'weeks' weeks
        that precede it.
        Then adjusts the training to have the same variance and mean as
        validation.

        Parameters
        ----------
        df: pd.Series
            The entire data frame containing the training set and the padding
            before it.
        validation: pd.Series
            The validation set.
        weeks: int
            The number of weeks that one wishes to use.

        Returns
        -------
        pd.Series
            The modified training set.
        """

        a = pd.concat(
            [df.shift(x).rename("-{}w".format(x//ppw)) for x in [ppw * i for i in range(0, weeks, 3)]],
            axis=1
        )

        start = self.training_time[0] - pd.DateOffset(weeks=1)
        end = self.validation_time[1] # Validation should be sanitized too.

        # Median of preceding weeks
        a = a[start:end].median(axis=1).interpolate()

        # Helpful for debugging
#        a.name = df.name

        # Normalize std
        a *= ((validation.std() + 1e-100) / (a.std() + 1e-100))
        # Normalize median
        a += validation.median() - a.median()

        return a

    def analyze_outages(self, potential_outages: pd.Series,
                        outages: Dict[str, str]) -> List:
        """
        Determines the amount of true and False positives inside a dataset.

        'potential_outages' is used to label outages in the time frame stored
        in 'self.test_time'. It is then compares to outages to see how many of
        them really were inside outages.

        Parameters
        ----------
        potential_outages: pd.Series
            Contains the list of alarms that were raised with an associated
            value that specifies how false our original prediction was.
        outages: Dict[str, str]
            A dict of time ranges of when outages happened to different
            signals.

        Returns
        -------
        List
            List containing a dictionary of the number of [TP, FP, TN, FN] for
            each threshold.
        """

        results_per_time_bin = []

        for time_bin in range(1, 6):
            res = {}

            for threshold in np.arange(
                    floor(potential_outages.min() * 10) / 10,
                    ceil(potential_outages.max() * 10) / 10,
                    0.1):

                # Each time bin is equal to False for now
                detected_outages = pd.Series(
                    False,
                    index=pd.date_range(self.validation_time[0],
                                        self.test_time[1],
                                        freq='{}h'.format(time_bin))
                )

                # Series containing a list of booleans stating if an outage
                # occurred during this time bin or not
                is_outage = potential_outages.resample(
                    '{}h'.format(time_bin)
                ).agg(lambda x: len(x[x > threshold]) > 0)

                # Set times with outages to True in the main series
                detected_outages[is_outage[is_outage].index] = True

                true_positives = 0
                false_positives = 0
                true_negatives = 0
                false_negatives = 0

                for signal, outage_dates in outages.items():
                    for event in outage_dates:
                        start = pd.to_datetime(event[0])
                        end = pd.to_datetime(event[1])

                        # Finding true positives
                        true_positives += len(
                            detected_outages[start:end][detected_outages]
                        )

                        # Finding false negatives
                        false_negatives += len(
                            detected_outages[start:end][~detected_outages]
                        )

                        # Finding false positives
                        false_positives += len(
                            detected_outages[:start][detected_outages].iloc[
                                :-1]
                        )
                        false_positives += len(
                            detected_outages[end:][detected_outages].iloc[1:]
                        )

                        # Finding true negatives
                        true_negatives += len(
                            detected_outages[:start][~detected_outages].iloc[
                                :-1]
                        )
                        true_negatives += len(
                            detected_outages[end:][~detected_outages].iloc[1:]
                        )

                res[threshold] = [
                    true_positives,
                    false_positives,
                    true_negatives,
                    false_negatives
                ]

            results_per_time_bin.append(res)

        return results_per_time_bin

    def prepare_histories(self, df, ppw):
        """
        Takes full history and generates history and diff_history for analysis.
        """

        start = self.training_time[0] - pd.DateOffset(weeks=1)
        end = self.validation_time[1] # Validation should be sanitized too.

        df[start:end] = self.sanitize_training(
            df[:end],
            df[self.validation_time[0]:self.validation_time[1]], ppw
        )

        # Difference df. The padding that precedes the data is used to make
        # sure that there are enough values to difference
        diff_df = self.difference(df.interpolate(), self.difference_order)

        # Remove the now useless padding
        df = df[self.training_time[0]:]
        diff_df = diff_df[self.training_time[0]:]

        return df, diff_df

    def prepare_analysis_wrap_queue(self, df, queue, name, outages=None):

        df, diff_df, best_config, mads = self.prepare_analysis(df, outages,
                                                               False)
        queue.put((best_config, mads, name))

    def prepare_analysis(self, df, ppw, outages=None, multiprocessing=True):
        """ Do the things in common between static and live analysis. """

        # Skip files that have already been processed
        for filename in os.listdir(self.results_dir):
            if '{}-{}-{}'.format(
                df.name,
                df[self.training_time[0]:].index[0].timestamp(),
                df.index[-1].timestamp(),
            ) in filename:
                logging.info('A file with the time series series already '
                             'exists: {}'.format(filename))
                return 0, (None,) * 4

        logging.info('Analyzing time series {}'.format(df.name))
        self.outages = outages

        df, diff_df = self.prepare_histories(df, ppw)

        if df.isnull().values.any():
            return 0, (None,) * 4

        # Perform initial tests on the two signals
        try:
            self.test_signal(df)
            self.test_signal(diff_df)
        except(LinAlgError):
            logging.error('Could not test signals')

        if self.plot:
            self.plot_signal(df)
            self.plot_signal(diff_df)

        df.to_csv(self.results_dir + "/" + df.name + ".raw")
        diff_df.to_csv(self.results_dir + "/" + df.name + ".diff")

        # Train different models and get the best one
        best_config, mads = self.find_best_model(df, diff_df, ppw,
                multiprocessing)

        if best_config == (-1, -1):
            logging.error('Failed to analyze signal {}.'.format(df.name))
            return 0, (None,) * 4

        return 1, (df, diff_df, best_config, mads)

    def live_analysis(self, complete_df: pd.DataFrame) -> None:
        """ Performs live analysis of a dataframe. """

        dfs = []
        diff_dfs = []
        best_configs = []
        mads = []
        queues = []
        futures = []
        names = []
        potential_outages = []

        pool = mp.Pool(4)

        # For loop that initializes all columns in df to be analyzed in live
        # mode.
        for i in range(len(complete_df.columns)):
            name = complete_df[complete_df.columns[i]].name

            df, diff_df, best_config, mad = self.prepare_analysis(
                                                            complete_df[name])

            if df is None:
                logging.error('Failed to configure for {}'.format(name))
                dfs.append(None)
                diff_dfs.append(None)
                best_configs.append(None)
                mads.append(None)
                queues.append(None)
                futures.append(None)
                names.append(name)
                potential_outages.append(None)
            else:
                # TODO
                # We keep the last day for testing purposes. Eventually, this
                # will be replaced by a fonction that gets new data from IODA
                future = df.iloc[-288:]
                df = df.iloc[:-288]

                queue_send = mp.Manager().Queue()
                queue_receive = mp.Manager().Queue()

                dfs.append(df)
                diff_dfs.append(diff_df)
                best_configs.append(best_config)
                mads.append(mad)
                queues.append([queue_send, queue_receive])
                futures.append(future)
                names.append(name)
                potential_outages.append(pd.Series())

                pool.apply_async(
                    self.walk_forward_validation,
                    args=(
                        df, diff_df, best_config, self.validation_time[0],
                        (queue_send, queue_receive), mad, 'live'
                    )
                )
                """ # Debug
                queue_send.put(future)
                self.walk_forward_validation(
                        df, diff_df, best_config, self.validation_time[0],
                        (queue_send, queue_receive), mad, 'live'
                )
                """

        # There are now processes working for every column in df. They will
        # send data when they finished analyzing the data they have and will
        # wait for additional work to be sent. This is the goal of this next
        # for loop.
        finished_processes = []
        while len(finished_processes) < len(complete_df.columns):
            for i in range(len(complete_df.columns)):
                if dfs[i] is None:
                    logging.debug('Skipping time series {}'.format(names[i]))
                    continue
                if i in finished_processes:
                    continue

                logging.debug('Main process is waiting for data from {}'
                              ''.format(names[i]))

                res = queues[i][1].get()
                logging.debug('Main process received data from {}'.format(
                    names[i]))

                if res is None:
                    finished_processes.append(i)
                    logging.debug('Finished analyzing {}'.format(names[i]))
                    continue

                # Append to series
                potential_outages[i] = pd.concat([potential_outages[i], res])
                potential_outages[i].to_pickle('live_results/{}.pkl'.format(
                    dfs[i].name.replace(' ', '')
                ))

                queues[i][0].put(futures[i])
                logging.debug('Main process sent data to {}'.format(names[i]))

                # TODO part where you are supposed to add in more data
                futures[i] = None

    def static_analysis(self, df: pd.DataFrame, outages) -> None:
        """
        Takes a signal as an input and detects potential outages.

        The signal which is contained in 'df' is analyzed during 'test_time'.
        Detected outages are true positives if they fall into one of the time
        ranges of 'outages'. The data is made stationary by differencing with
        'difference_order', ARMA parameters are computed so that (p + q <
        'arima_order'), and predictions are made 'n' steps at a time.

        Parameters
        ----------
        df: pd.DataFrame
            Contains data for a signal during 'test_time'.
        outages: List[List[str]]
            List of lists of two elements that are time ranges for the periods
            where outages are occurring.

        Returns
        -------
        dict
            A dictionary containing True Positives and False Positives for
            different thresholds.
        """

        df, diff_df, best_config, mads = self.prepare_analysis(df, outages)
        if not df:
            return None

        logging.debug('Starting to evaluate the test set with the best model.')

        potential_outages = self.walk_forward_validation(
            df, diff_df, best_config, self.validation_time[0], None, mads,
            'test'
        )
        while potential_outages is None and best_config[1] > 0:
            best_config = (best_config[0], best_config[1] - 1)
            logging.debug('Retrying to analyze the test set with {}'
                          ''.format(best_config))

            potential_outages = self.walk_forward_validation(
                df, diff_df, best_config, self.validation_time[0], None, mads,
                'test'
            )

        if potential_outages is None:
            logging.error('Could not analyze the test set.')
            return None

        if potential_outages is None:
            logging.error('The ARMA model did not converge on the test set.')
            return None
        else:
            logging.info('Found {} potential outages.'
                         ''.format(len(potential_outages))
                         )

        # Filename in which the results are going to be stored
        filename = '{}-{}-{}-{}-{}-{},{}.pkl'.format(
            int(df[self.training_time[0]:].median()),
            df.name,
            df[self.training_time[0]:].index[0].timestamp(),
            df.index[-1].timestamp(),
            bool(outages),  # Say if the dataset contains outages or not
            best_config[0], best_config[1]
        )

        logging.debug('Saving results to {}'.format(filename))
        potential_outages.to_pickle(self.results_dir + filename)
