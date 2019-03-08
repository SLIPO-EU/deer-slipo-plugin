package org.aksw.deer.plugin.slipo.decorator;

import org.aksw.deer.DeerExecutionGraphNode;
import org.aksw.deer.ParameterizedDeerExecutionGraphNode;
import org.aksw.deer.decorators.AbstractDeerExecutionGraphNodeDecorator;
import org.aksw.deer.decorators.AbstractDeerExecutionGraphNodeWrapper;
import org.aksw.deer.decorators.AbstractParameterizedDeerExecutionGraphNodeDecorator;
import org.aksw.deer.decorators.SparqlAnalyticsWrapper;
import org.aksw.faraday_cage.engine.ExecutionGraphNode;
import org.aksw.faraday_cage.engine.Parameterized;
import org.apache.jena.rdf.model.*;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
@Extension
public class SlipoAnalyticsWrapper extends AbstractDeerExecutionGraphNodeWrapper {

  private static final Logger logger = LoggerFactory.getLogger(SparqlAnalyticsWrapper.class);
  public static final String SLIPO_NS = "http://slipo.eu/def#";

  private void applyTriggered(List<Model> in, List<Model> out) {
    final AtomicLong triples = new AtomicLong(0);
    final AtomicLong pois = new AtomicLong(0);
    in.forEach(m -> {
      // every POI will have exactly one category
      Property category = ResourceFactory.createProperty(SLIPO_NS + "category");
      long poiCount = m.listStatements(null, category, (RDFNode) null).toModel().size();
      pois.addAndGet(poiCount);
      // if dataset has POIs, add up number of total triples
      if (poiCount > 0) {
        triples.addAndGet(m.size());
      }
    });
  }

  @Override
  public DeerExecutionGraphNode wrap(DeerExecutionGraphNode executionNode) {
    if (executionNode instanceof Parameterized) {
      return new ParameterizedSlipoAnalyticsDecorator((ParameterizedDeerExecutionGraphNode) executionNode);
    } else {
      return new SlipoAnalyticsDecorator(executionNode);
    }
  }

  private class SlipoAnalyticsDecorator extends AbstractDeerExecutionGraphNodeDecorator {

    public SlipoAnalyticsDecorator(ExecutionGraphNode<Model> other) {
      super(other);
    }

    public List<Model> apply(List<Model> in) {
      List<Model> out = super.apply(in);
      applyTriggered(in, out);
      return out;
    }

  }

  private class ParameterizedSlipoAnalyticsDecorator extends AbstractParameterizedDeerExecutionGraphNodeDecorator {

    public ParameterizedSlipoAnalyticsDecorator(ParameterizedDeerExecutionGraphNode other) {
      super(other);
    }

    public List<Model> apply(List<Model> in) {
      List<Model> out = super.apply(in);
      applyTriggered(in, out);
      return out;
    }

  }

}
