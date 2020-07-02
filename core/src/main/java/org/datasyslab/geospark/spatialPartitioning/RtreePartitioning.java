/*
 * FILE: RtreePartitioning
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.spatialPartitioning;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.Boundable;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class RtreePartitioning.
 */
public class RtreePartitioning
        implements Serializable
{

    /**
     * A class that adds query bounds to STRtree
     */
    private class QueryableSTRtree extends STRtree {
        public QueryableSTRtree(int nodeCapacity) {
            super(nodeCapacity);
        }
        
        public List<Envelope> queryBoundary() {
            build();
            List<Envelope> boundaries = new ArrayList();
            queryBoundary(root, boundaries);	    
            return boundaries;
        }

        private void queryBoundary(AbstractNode node, List<Envelope> matches) {
            List childBoundables = node.getChildBoundables();
            if (node.getLevel() == 0) {
                matches.add((Envelope)node.getBounds());
            } else {
                for (int i = 0; i < childBoundables.size(); i++) {
                    Boundable childBoundable = (Boundable) childBoundables.get(i);
                    if (childBoundable instanceof AbstractNode) {
                        queryBoundary((AbstractNode) childBoundable, matches);
                    }
                }
            }
        }
    }


    /**
     * The grids.
     */
    final List<Envelope> grids = new ArrayList<>();

    /**
     * Instantiates a new rtree partitioning.
     *
     * @param samples the sample list
     * @param partitions the partitions
     * @throws Exception the exception
     */
    public RtreePartitioning(List<Envelope> samples, int partitions)
            throws Exception
    {
        QueryableSTRtree strtree = new QueryableSTRtree(samples.size() / partitions);
        for (Envelope sample : samples) {
            strtree.insert(sample, sample);
        }

        List<Envelope> envelopes = strtree.queryBoundary();
        for (Envelope envelope : envelopes) {
            grids.add(envelope);
        }
    }

    /**
     * Gets the grids.
     *
     * @return the grids
     */
    public List<Envelope> getGrids()
    {

        return this.grids;
    }
}
