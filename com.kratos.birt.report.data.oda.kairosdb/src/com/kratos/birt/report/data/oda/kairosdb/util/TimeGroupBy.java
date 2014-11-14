package com.kratos.birt.report.data.oda.kairosdb.util;

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
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class TimeGroupBy implements GroupBy
{
	@NotNull
	private Duration rangeSize;

	@Min(1)
	private int groupCount;


	public TimeGroupBy()
	{
	}

	public TimeGroupBy(Duration rangeSize, int groupCount)
	{
		checkArgument(groupCount > 0);

		this.rangeSize = checkNotNull(rangeSize);
		this.groupCount = groupCount;
	}


	public void setRangeSize(Duration rangeSize)
	{
		this.rangeSize = rangeSize;
	}

	public void setGroupCount(int groupCount)
	{
		this.groupCount = groupCount;
	}

	@Override
	public void addToSet(Set<String> tagSet, Set<Integer> valueSet,
			Set<Duration> timeSet) {
		timeSet.add(rangeSize);
		
	}



}