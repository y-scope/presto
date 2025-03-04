package com.yscope.presto;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.util.stream.Collectors.toMap;

public class TestClpQueryBase {
    protected static final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
    protected static final StandardFunctionResolution standardFunctionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    protected static final Metadata metadata = MetadataManager.createTestMetadataManager();

    protected static ClpColumnHandle regionId = new ClpColumnHandle("region.Id", BIGINT, true);
    protected static ClpColumnHandle regionName = new ClpColumnHandle("region.Name", VARCHAR, true);
    protected static ClpColumnHandle city = new ClpColumnHandle("city", VARCHAR, true);
    protected static final ClpColumnHandle fare = new ClpColumnHandle("fare", DOUBLE, true);
    protected static final ClpColumnHandle isHoliday = new ClpColumnHandle("isHoliday", BOOLEAN, true);
    protected static final Map<VariableReferenceExpression, ColumnHandle> variableToColumnHandleMap =
            Stream.of(regionId, regionName, city, fare, isHoliday)
                    .collect(toMap(
                            ch -> new VariableReferenceExpression(Optional.empty(), ch.getColumnName(), ch.getColumnType()),
                            ch -> ch
                    ));
    protected final TypeProvider typeProvider = TypeProvider.fromVariables(variableToColumnHandleMap.keySet());

    protected static class SessionHolder {
        private final ConnectorSession connectorSession;
        private final Session session;

        public SessionHolder() {
            connectorSession = SESSION;
            session = TestingSession.testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties().getSessionProperties())).build();
        }

        public ConnectorSession getConnectorSession() {
            return connectorSession;
        }

        public Session getSession() {
            return session;
        }
    }

    public static Expression expression(String sql) {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
    }

    protected RowExpression toRowExpression(Expression expression, Session session) {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                new SqlParser(),
                typeProvider,
                expression,
                ImmutableMap.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(expression, expressionTypes, ImmutableMap.of(), functionAndTypeManager, session);
    }

    protected RowExpression getRowExpression(String sqlExpression, SessionHolder sessionHolder) {
        return toRowExpression(expression(sqlExpression), sessionHolder.getSession());
    }
}
