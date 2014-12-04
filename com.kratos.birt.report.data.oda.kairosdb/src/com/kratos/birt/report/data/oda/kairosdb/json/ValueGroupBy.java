package com.kratos.birt.report.data.oda.kairosdb.json;

/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */



import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import javax.validation.constraints.Min;

/**
 * Groups data points by value. Data points are a range of values specified by range size.
 */
public class ValueGroupBy implements GroupBy
{
	@Min(1)
	private int rangeSize;

	public ValueGroupBy()
	{
	}

	public ValueGroupBy(int rangeSize)
	{
		checkArgument(rangeSize > 0);

		this.rangeSize = rangeSize;
	}

	public void setRangeSize(int rangeSize)
	{
		this.rangeSize = rangeSize;
	}

	@Override
	public void addToSet(Set<String> tagSet, Set<Integer> valueSet,
			Set<Duration> timeSet) {
		valueSet.add(rangeSize);
		
	}
}