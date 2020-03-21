/*
 * Copyright 2018 - 2020 Blazebit.
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

package com.blazebit.job.view.model;

import com.blazebit.job.jpa.model.JobConfiguration;
import com.blazebit.job.jpa.model.ParameterSerializable;
import com.blazebit.job.jpa.model.TimeFrame;
import com.blazebit.persistence.view.EntityView;
import com.blazebit.persistence.view.PreUpdate;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A entity view based implementation for the {@link com.blazebit.job.JobConfiguration} interface with support for dirty tracking.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
@EntityView(JobConfiguration.class)
public abstract class JobConfigurationView implements com.blazebit.job.JobConfiguration, Serializable {

    private final DirtyMarkingSet<TimeFrame> executionTimeFrames;
    private final DirtyMarkingMap<String, Serializable> parameters;

    /**
     * Creates a new job configuration view.
     */
    public JobConfigurationView() {
        ParameterSerializable o = (ParameterSerializable) getParameterSerializable();
        if (o != null && o.getExecutionTimeFrames() != null) {
            this.executionTimeFrames = new DirtyMarkingSet<>(o.getExecutionTimeFrames());
        } else {
            this.executionTimeFrames = new DirtyMarkingSet<>(new HashSet<>(0));
        }
        if (o != null && o.getParameters() != null) {
            this.parameters = new DirtyMarkingMap<>(o.getParameters());
        } else {
            this.parameters = new DirtyMarkingMap<>(new HashMap<>(0));
        }
    }

    @Override
    public final Set<TimeFrame> getExecutionTimeFrames() {
        return executionTimeFrames;
    }

    @Override
    public final Map<String, Serializable> getParameters() {
        return parameters;
    }

    /**
     * Returns the parameter serializable object.
     *
     * @return the parameter serializable object
     */
    abstract Serializable getParameterSerializable();

    /**
     * Sets the parameter serializable object.
     *
     * @param parameterSerializable The parameter serializable object
     */
    abstract void setParameterSerializable(Serializable parameterSerializable);

    /**
     * The pre-update method.
     */
    @PreUpdate
    protected void preUpdate() {
        setParameterSerializable(new ParameterSerializable(executionTimeFrames.getDelegate(), parameters.getDelegate()));
    }

    /* We need the following collections to set parameterSerializable to null to simulate dirty marking so that the preUpdate method gets executed */

    /**
     * A {@link Map} that marks the parameter serializable as dirty on changes.
     *
     * @author Christian Beikov
     * @since 1.0.0
     */
    private class DirtyMarkingMap<K, V> extends DelegatingMap<K, V> {

        public DirtyMarkingMap(Map<K, V> delegate) {
            super(delegate);
        }

        @Override
        public V put(K key, V value) {
            V oldValue = super.put(key, value);
            if (!Objects.equals(oldValue, value)) {
                setParameterSerializable(null);
            }
            return oldValue;
        }

        @Override
        public V remove(Object key) {
            setParameterSerializable(null);
            return super.remove(key);
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            setParameterSerializable(null);
            super.putAll(m);
        }

        @Override
        public void clear() {
            setParameterSerializable(null);
            super.clear();
        }

        @Override
        public Set<K> keySet() {
            return new DirtyMarkingSet<>(super.keySet());
        }

        @Override
        public Collection<V> values() {
            return new DirtyMarkingCollection<>(super.values());
        }

        @Override
        public Set<Map.Entry<K, V>> entrySet() {
            return new DirtyMarkingSet<>(super.entrySet());
        }
    }

    /**
     * A {@link Iterator} that marks the parameter serializable as dirty on changes.
     *
     * @author Christian Beikov
     * @since 1.0.0
     */
    private class DirtyMarkingIterator<T> extends DelegatingIterator<T> {
        public DirtyMarkingIterator(Iterator<T> delegate) {
            super(delegate);
        }

        @Override
        public void remove() {
            super.remove();
            setParameterSerializable(null);
        }
    }

    /**
     * A {@link Collection} that marks the parameter serializable as dirty on changes.
     *
     * @author Christian Beikov
     * @since 1.0.0
     */
    private class DirtyMarkingCollection<T> extends DelegatingCollection<T> {
        public DirtyMarkingCollection(Collection<T> delegate) {
            super(delegate);
        }

        @Override
        public Iterator<T> iterator() {
            return new DirtyMarkingIterator<>(super.iterator());
        }

        @Override
        public boolean add(T t) {
            setParameterSerializable(null);
            return super.add(t);
        }

        @Override
        public boolean remove(Object o) {
            setParameterSerializable(null);
            return super.remove(o);
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            setParameterSerializable(null);
            return super.addAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            setParameterSerializable(null);
            return super.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            setParameterSerializable(null);
            return super.removeAll(c);
        }

        @Override
        public void clear() {
            setParameterSerializable(null);
            super.clear();
        }
    }

    /**
     * A {@link Set} that marks the parameter serializable as dirty on changes.
     *
     * @author Christian Beikov
     * @since 1.0.0
     */
    private class DirtyMarkingSet<T> extends DirtyMarkingCollection<T> implements Set<T> {
        public DirtyMarkingSet(Set<T> delegate) {
            super(delegate);
        }

        @Override
        public Set<T> getDelegate() {
            return (Set<T>) super.getDelegate();
        }
    }

}