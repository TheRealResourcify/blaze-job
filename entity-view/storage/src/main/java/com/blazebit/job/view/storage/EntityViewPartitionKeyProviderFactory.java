/*
 * Copyright 2018 - 2023 Blazebit.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blazebit.job.view.storage;

import com.blazebit.apt.service.ServiceProvider;
import com.blazebit.job.ConfigurationSource;
import com.blazebit.job.spi.PartitionKeyProvider;
import com.blazebit.job.spi.PartitionKeyProviderFactory;

/**
 * A factory for {@link EntityViewPartitionKeyProvider}.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
@ServiceProvider(PartitionKeyProviderFactory.class)
public class EntityViewPartitionKeyProviderFactory implements PartitionKeyProviderFactory {

    @Override
    public PartitionKeyProvider createPartitionKeyProvider(com.blazebit.job.ServiceProvider serviceProvider, ConfigurationSource configurationSource) {
        return new EntityViewPartitionKeyProvider(serviceProvider, configurationSource);
    }

}
