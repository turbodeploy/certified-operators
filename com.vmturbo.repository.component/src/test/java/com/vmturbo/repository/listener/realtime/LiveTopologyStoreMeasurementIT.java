package com.vmturbo.repository.listener.realtime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.metrics.MemoryMetricsManager;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * This test is ignored for automatic builds.
 *
 * It's intended to measure the memory usage (and other performance) of a topology graph with a
 * large customer topology.
 */
@Ignore
public class LiveTopologyStoreMeasurementIT {
    private final Logger logger = LogManager.getLogger();

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator =
            new GlobalSupplyChainCalculator();

    private static String BIG_QUERY_JSON =
            "{\n" + "  \"searchParameters\": [{\n" + "    \"startingFilter\": {\n"
                    + "      \"propertyName\": \"entityType\",\n" + "      \"numericFilter\": {\n"
                    + "        \"comparisonOperator\": \"EQ\",\n" + "        \"value\": \"10\"\n"
                    + "      }\n" + "    },\n"
                    + "\"searchFilter\": [{\n"
                    + "      \"propertyFilter\": {\n" + "        \"propertyName\": \"tags\",\n"
                    + "        \"mapFilter\": {\n" + "          \"key\": \"aitNumber\",\n"
                    + "          \"values\": [\"1022\", \"1050\", \"1050\", \"10611\", \"1067\", \"1069\", \"10705\", \"10720\", \"10761\", \"10761\", \"11047\", \"1113\", \"11356\", \"11663\", \"11825\", \"11914\", \"11953\", \"12302\", \"12302\", \"12303\", \"12338\", \"12395\", \"12399\", \"12399\", \"12481\", \"12650\", \"12795\", \"12847\", \"1284\", \"12864\", \"12886\", \"12888\", \"12910\", \"1293\", \"12959\", \"12961\", \"13017\", \"13029\", \"13039\", \"1341\", \"1342\", \"1346\", \"14021\", \"14128\", \"14330\", \"14436\", \"14516\", \"1453\", \"14547\", \"14549\", \"14599\", \"14599\", \"14801\", \"14875\", \"14952\", \"14972\", \"14972\", \"15021\", \"15056\", \"15126\", \"1532\", \"1532\", \"15689\", \"15756\", \"15759\", \"15759\", \"1578\", \"16091\", \"16417\", \"16698\", \"16982\", \"16988\", \"1706\", \"17127\", \"17132\", \"1714\", \"17159\", \"1721\", \"17303\", \"17386\", \"1741\", \"17461\", \"17461\", \"1754\", \"17602\", \"17634\", \"17647\", \"17986\", \"1819\", \"18212\", \"18869\", \"18878\", \"18902\", \"19078\", \"19078\", \"19126\", \"19126\", \"19126\", \"19416\", \"1959\", \"19638\", \"19638\", \"19806\", \"1994\", \"19950\", \"20021\", \"20300\", \"20339\", \"20419\", \"20686\", \"20706\", \"20706\", \"20706\", \"20743\", \"20749\", \"20764\", \"20765\", \"20782\", \"20796\", \"20827\", \"20857\", \"20867\", \"20876\", \"20892\", \"20896\", \"2089\", \"20909\", \"20914\", \"20917\", \"20918\", \"20924\", \"20924\", \"20925\", \"20929\", \"20933\", \"20933\", \"2095\", \"20973\", \"21010\", \"21035\", \"21036\", \"21050\", \"21054\", \"21060\", \"21066\", \"21072\", \"21076\", \"21085\", \"21091\", \"21135\", \"21152\", \"21156\", \"21159\", \"21160\", \"21163\", \"21166\", \"21173\", \"21173\", \"21175\", \"21176\", \"21186\", \"21195\", \"21196\", \"21203\", \"21206\", \"21214\", \"21215\", \"21226\", \"21230\", \"21239\", \"21252\", \"21277\", \"21281\", \"21287\", \"21304\", \"21305\", \"21308\", \"21310\", \"21311\", \"21312\", \"21315\", \"21317\", \"21318\", \"21369\", \"21404\", \"21514\", \"21527\", \"21597\", \"21648\", \"21659\", \"21667\", \"22034\", \"22036\", \"22174\", \"22218\", \"22255\", \"22279\", \"2227\", \"22286\", \"22359\", \"22421\", \"22525\", \"22541\", \"22602\", \"22602\", \"22764\", \"22799\", \"2316\", \"23240\", \"23245\", \"23247\", \"23324\", \"23374\", \"23384\", \"2338\", \"23397\", \"23415\", \"23427\", \"23439\", \"23449\", \"23450\", \"23451\", \"23466\", \"23476\", \"23485\", \"23493\", \"23513\", \"23513\", \"23536\", \"23541\", \"23546\", \"23558\", \"23568\", \"23572\", \"23573\", \"23582\", \"23582\", \"23582\", \"23595\", \"23597\", \"23600\", \"23615\", \"23624\", \"23663\", \"23693\", \"23695\", \"23707\", \"23728\", \"23729\", \"23731\", \"23736\", \"23755\", \"23756\", \"23758\", \"23763\", \"23784\", \"23799\", \"23802\", \"23817\", \"23826\", \"23828\", \"23837\", \"23854\", \"23859\", \"23878\", \"23888\", \"23971\", \"23989\", \"23995\", \"24005\", \"24007\", \"24024\", \"24030\", \"24057\", \"24060\", \"24073\", \"24081\", \"24089\", \"24106\", \"24134\", \"24135\", \"24137\", \"24137\", \"24139\", \"24147\", \"24156\", \"24168\", \"24182\", \"24185\", \"24199\", \"2419\", \"24202\", \"24270\", \"24270\", \"24296\", \"24302\", \"24306\", \"24345\", \"24371\", \"24378\", \"24406\", \"24418\", \"24422\", \"24476\", \"24479\", \"24486\", \"24495\", \"24503\", \"24511\", \"24553\", \"24601\", \"24606\", \"24619\", \"24652\", \"24658\", \"24667\", \"24687\", \"24707\", \"24722\", \"24724\", \"24748\", \"24761\", \"24766\", \"24767\", \"24780\", \"24790\", \"24827\", \"24857\", \"24875\", \"24894\", \"24894\", \"24895\", \"24903\", \"24929\", \"24937\", \"24971\", \"25005\", \"25027\", \"25029\", \"25042\", \"25044\", \"25044\", \"25049\", \"25056\", \"25064\", \"25070\", \"25085\", \"25109\", \"25171\", \"25184\", \"25193\", \"25220\", \"25241\", \"25268\", \"25304\", \"25363\", \"25398\", \"25398\", \"25421\", \"25446\", \"25465\", \"25476\", \"25493\", \"25502\", \"25507\", \"25524\", \"25529\", \"25533\", \"25538\", \"25539\", \"25569\", \"25576\", \"25588\", \"25598\", \"25605\", \"25633\", \"25641\", \"25667\", \"25731\", \"25813\", \"25834\", \"25854\", \"25880\", \"25899\", \"25975\", \"25988\", \"25991\", \"26106\", \"26107\", \"26115\", \"26117\", \"26172\", \"26173\", \"26195\", \"26199\", \"26205\", \"26217\", \"26270\", \"26280\", \"26284\", \"26342\", \"26353\", \"26399\", \"26451\", \"26457\", \"26482\", \"26495\", \"26560\", \"26595\", \"26648\", \"26649\", \"26667\", \"26669\", \"26685\", \"26712\", \"26719\", \"26833\", \"26932\", \"26970\", \"26970\", \"27056\", \"27224\", \"27436\", \"27679\", \"27920\", \"2803\", \"28140\", \"28209\", \"28526\", \"28687\", \"28688\", \"28708\", \"28718\", \"28727\", \"28795\", \"28840\", \"29188\", \"29209\", \"29387\", \"29412\", \"29508\", \"29549\", \"2976\", \"29787\", \"29904\", \"30067\", \"30219\", \"30620\", \"306\", \"30726\", \"31138\", \"31159\", \"31265\", \"31347\", \"3252\", \"3252\", \"32589\", \"32657\", \"32664\", \"32758\", \"32759\", \"32910\", \"33810\", \"34310\", \"34381\", \"34397\", \"352\", \"35587\", \"35655\", \"35857\", \"36223\", \"36732\", \"36732\", \"3684\", \"36998\", \"37003\", \"37004\", \"37076\", \"37077\", \"3711\", \"37338\", \"37378\", \"3781\", \"37852\", \"37938\", \"38087\", \"383\", \"383\", \"38420\", \"38548\", \"38637\", \"38791\", \"39057\", \"39147\", \"39182\", \"39432\", \"39433\", \"39889\", \"39892\", \"39892\", \"39977\", \"40168\", \"40198\", \"40208\", \"4025\", \"4025\", \"40425\", \"40472\", \"40509\", \"40701\", \"40757\", \"4075\", \"40856\", \"40897\", \"40909\", \"41023\", \"41094\", \"4114\", \"4114\", \"41268\", \"4152\", \"41707\", \"41821\", \"41923\", \"4204\", \"4208\", \"4213\", \"42393\", \"42852\", \"42919\", \"43189\", \"43499\", \"43809\", \"43889\", \"43989\", \"44179\", \"44679\", \"44679\", \"45099\", \"45222\", \"45224\", \"45228\", \"45411\", \"454\", \"4562\", \"45631\", \"45691\", \"45703\", \"45851\", \"4601\", \"4603\", \"46141\", \"46252\", \"46263\", \"46401\", \"46572\", \"46611\", \"46711\", \"46901\", \"46911\", \"469\", \"47021\", \"47061\", \"47152\", \"47191\", \"47231\", \"47401\", \"47581\", \"47591\", \"47791\", \"48001\", \"48051\", \"48082\", \"48161\", \"48172\", \"48191\", \"48261\", \"48291\", \"48291\", \"48311\", \"48351\", \"48351\", \"48351\", \"48352\", \"48472\", \"48472\", \"48521\", \"48561\", \"48842\", \"48842\", \"48883\", \"48951\", \"48971\", \"49061\", \"49111\", \"49153\", \"49301\", \"4970\", \"4973\", \"49741\", \"49742\", \"4974\", \"49892\", \"49902\", \"50072\", \"50073\", \"50141\", \"50161\", \"50231\", \"50491\", \"50691\", \"50763\", \"50781\", \"50901\", \"51091\", \"51141\", \"51151\", \"51322\", \"51351\", \"51391\", \"51421\", \"51492\", \"51672\", \"51831\", \"52221\", \"52241\", \"52281\", \"52281\", \"52331\", \"52351\", \"52563\", \"52632\", \"52701\", \"52782\", \"53202\", \"53281\", \"53311\", \"53371\", \"53391\", \"53411\", \"53421\", \"53531\", \"53561\", \"53611\", \"53681\", \"53681\", \"53711\", \"53761\", \"53892\", \"53931\", \"54021\", \"54032\", \"54082\", \"54191\", \"54221\", \"54231\", \"54581\", \"54601\", \"5466\", \"54782\", \"54791\", \"54881\", \"55031\", \"55091\", \"5518\", \"55231\", \"55251\", \"55371\", \"55541\", \"55821\", \"55851\", \"55921\", \"55951\", \"56401\", \"56462\", \"56601\", \"56612\", \"56851\", \"56871\", \"56951\", \"56\", \"57061\", \"57132\", \"57222\", \"57552\", \"57561\", \"5756\", \"57661\", \"57732\", \"57754\", \"57771\", \"57911\", \"5801\", \"58131\", \"58204\", \"5835\", \"58413\", \"58441\", \"58611\", \"5874\", \"58811\", \"58861\", \"5894\", \"58951\", \"58962\", \"58965\", \"59315\", \"59335\", \"59396\", \"59485\", \"5950\", \"59655\", \"59656\", \"59915\", \"59925\", \"60035\", \"60115\", \"60176\", \"60195\", \"60245\", \"60255\", \"60265\", \"60266\", \"60267\", \"60485\", \"60486\", \"60487\", \"60515\", \"60525\", \"60565\", \"60595\", \"60611\", \"60696\", \"60698\", \"6071\", \"60755\", \"60805\", \"60876\", \"60987\", \"60988\", \"6104\", \"61215\", \"61266\", \"61275\", \"61365\", \"61435\", \"61585\", \"61625\", \"61635\", \"61677\", \"61818\", \"61855\", \"61888\", \"61889\", \"61895\", \"61897\", \"61965\", \"61965\", \"62095\", \"62285\", \"62465\", \"62716\", \"62736\", \"62751\", \"62895\", \"62956\", \"63066\", \"63105\", \"6317\", \"63285\", \"63325\", \"6335\", \"63437\", \"63505\", \"63506\", \"63526\", \"63566\", \"63567\", \"63598\", \"63786\", \"63816\", \"63816\", \"63905\", \"64035\", \"64105\", \"64198\", \"64276\", \"6433\", \"64386\", \"64397\", \"64445\", \"64448\", \"64545\", \"64586\", \"64586\", \"64897\", \"64899\", \"65106\", \"65115\", \"65135\", \"65165\", \"6516\", \"65173\", \"65275\", \"65415\", \"65416\", \"65436\", \"65507\", \"66220\", \"66220\", \"66245\", \"66337\", \"66458\", \"66476\", \"66505\", \"67086\", \"67195\", \"67235\", \"67325\", \"6734\", \"67475\", \"67617\", \"67617\", \"676\", \"67815\", \"68025\", \"68125\", \"68125\", \"68335\", \"6841\", \"68665\", \"68747\", \"68815\", \"68816\", \"68987\", \"68998\", \"69278\", \"69278\", \"69279\", \"69335\", \"69355\", \"69375\", \"69420\", \"69437\", \"69590\", \"69590\", \"69599\", \"69599\", \"69603\", \"69624\", \"69625\", \"69626\", \"69642\", \"69652\", \"69661\", \"69692\", \"69692\", \"69712\", \"69714\", \"69744\", \"69755\", \"69765\", \"69772\", \"69793\", \"69802\", \"69819\", \"69820\", \"69838\", \"69840\", \"69850\", \"69850\", \"69858\", \"69872\", \"69880\", \"69904\", \"69913\", \"69917\", \"69960\", \"69960\", \"69965\", \"69966\", \"69969\", \"69971\", \"69988\", \"69992\", \"69995\", \"69999\", \"70020\", \"70035\", \"70038\", \"70040\", \"70041\", \"70043\", \"70054\", \"70055\", \"70078\", \"70081\", \"70096\", \"70102\", \"70185\", \"70185\", \"70214\", \"70228\", \"70242\", \"70249\", \"70255\", \"70280\", \"70344\", \"70351\", \"70354\", \"70365\", \"70395\", \"70395\", \"70405\", \"70406\", \"70406\", \"70407\", \"70418\", \"70419\", \"70434\", \"70438\", \"70442\", \"70447\", \"70481\", \"70483\", \"70487\", \"70488\", \"70507\", \"70507\", \"70514\", \"70515\", \"70525\", \"70529\", \"70532\", \"70533\", \"70557\", \"70563\", \"70575\", \"70581\", \"70588\", \"70591\", \"70601\", \"70611\", \"70616\", \"70639\", \"70651\", \"70653\", \"70660\", \"70686\", \"70699\", \"70700\", \"70710\", \"70730\", \"70731\", \"70735\", \"70772\", \"70793\", \"70793\", \"70819\", \"70838\", \"70845\", \"70846\", \"70848\", \"70856\", \"70857\", \"70857\", \"70862\", \"70872\", \"70890\", \"70899\", \"70900\", \"70914\", \"71004\", \"71021\", \"71027\", \"71076\", \"71091\", \"71102\", \"71125\", \"71133\", \"71158\", \"71159\", \"71160\", \"71163\", \"71164\", \"71181\", \"71192\", \"71193\", \"71201\", \"71214\", \"71229\", \"71257\", \"71262\", \"71264\", \"71277\", \"71296\", \"71330\", \"71338\", \"71341\", \"71347\", \"71377\", \"7142\", \"71442\", \"71451\", \"71454\", \"71466\", \"71497\", \"71504\", \"71505\", \"71511\", \"71515\", \"71541\", \"71551\", \"71552\", \"71610\", \"71612\", \"71630\", \"71635\", \"71636\", \"71651\", \"71689\", \"71690\", \"71691\", \"71692\", \"71693\", \"71695\", \"71697\", \"71700\", \"71701\", \"71703\", \"71751\", \"71763\", \"71764\", \"71786\", \"71787\", \"71788\", \"71813\", \"71836\", \"71842\", \"71880\", \"71896\", \"71899\", \"71949\", \"71965\", \"71983\", \"72002\", \"7446\", \"7460\", \"7485\", \"7504\", \"7519\", \"7532\", \"7534\", \"7535\", \"7584\", \"7640\", \"7653\", \"7673\", \"7677\", \"7683\", \"7696\", \"7726\", \"7768\", \"7770\", \"7925\", \"8022\", \"8156\", \"8363\", \"8421\", \"855\", \"8561\", \"8581\", \"8581\", \"8638\", \"8731\", \"8733\", \"8740\", \"8754\", \"8765\", \"8794\", \"8805\", \"8868\", \"8881\", \"8892\", \"8906\", \"8938\", \"8993\", \"9005\", \"906\", \"9102\", \"9134\", \"9303\", \"9347\", \"9359\", \"9537\", \"956\", \"95\", \"9605\", \"9710\", \"9851\", \"986\"]\n"
                    + "        }\n" + "      }\n" + "    }],\n" + "    \"sourceFilterSpecs\": {\n"
                    + "      \"expressionType\": \"EQ\",\n"
                    + "      \"expressionValue\": \"aitNumber\\u003d1022|aitNumber\\u003d1050|aitNumber\\u003d1050|aitNumber\\u003d10611|aitNumber\\u003d1067|aitNumber\\u003d1069|aitNumber\\u003d10705|aitNumber\\u003d10720|aitNumber\\u003d10761|aitNumber\\u003d10761|aitNumber\\u003d11047|aitNumber\\u003d1113|aitNumber\\u003d11356|aitNumber\\u003d11663|aitNumber\\u003d11825|aitNumber\\u003d11914|aitNumber\\u003d11953|aitNumber\\u003d12302|aitNumber\\u003d12302|aitNumber\\u003d12303|aitNumber\\u003d12338|aitNumber\\u003d12395|aitNumber\\u003d12399|aitNumber\\u003d12399|aitNumber\\u003d12481|aitNumber\\u003d12650|aitNumber\\u003d12795|aitNumber\\u003d12847|aitNumber\\u003d1284|aitNumber\\u003d12864|aitNumber\\u003d12886|aitNumber\\u003d12888|aitNumber\\u003d12910|aitNumber\\u003d1293|aitNumber\\u003d12959|aitNumber\\u003d12961|aitNumber\\u003d13017|aitNumber\\u003d13029|aitNumber\\u003d13039|aitNumber\\u003d1341|aitNumber\\u003d1342|aitNumber\\u003d1346|aitNumber\\u003d14021|aitNumber\\u003d14128|aitNumber\\u003d14330|aitNumber\\u003d14436|aitNumber\\u003d14516|aitNumber\\u003d1453|aitNumber\\u003d14547|aitNumber\\u003d14549|aitNumber\\u003d14599|aitNumber\\u003d14599|aitNumber\\u003d14801|aitNumber\\u003d14875|aitNumber\\u003d14952|aitNumber\\u003d14972|aitNumber\\u003d14972|aitNumber\\u003d15021|aitNumber\\u003d15056|aitNumber\\u003d15126|aitNumber\\u003d1532|aitNumber\\u003d1532|aitNumber\\u003d15689|aitNumber\\u003d15756|aitNumber\\u003d15759|aitNumber\\u003d15759|aitNumber\\u003d1578|aitNumber\\u003d16091|aitNumber\\u003d16417|aitNumber\\u003d16698|aitNumber\\u003d16982|aitNumber\\u003d16988|aitNumber\\u003d1706|aitNumber\\u003d17127|aitNumber\\u003d17132|aitNumber\\u003d1714|aitNumber\\u003d17159|aitNumber\\u003d1721|aitNumber\\u003d17303|aitNumber\\u003d17386|aitNumber\\u003d1741|aitNumber\\u003d17461|aitNumber\\u003d17461|aitNumber\\u003d1754|aitNumber\\u003d17602|aitNumber\\u003d17634|aitNumber\\u003d17647|aitNumber\\u003d17986|aitNumber\\u003d1819|aitNumber\\u003d18212|aitNumber\\u003d18869|aitNumber\\u003d18878|aitNumber\\u003d18902|aitNumber\\u003d19078|aitNumber\\u003d19078|aitNumber\\u003d19126|aitNumber\\u003d19126|aitNumber\\u003d19126|aitNumber\\u003d19416|aitNumber\\u003d1959|aitNumber\\u003d19638|aitNumber\\u003d19638|aitNumber\\u003d19806|aitNumber\\u003d1994|aitNumber\\u003d19950|aitNumber\\u003d20021|aitNumber\\u003d20300|aitNumber\\u003d20339|aitNumber\\u003d20419|aitNumber\\u003d20686|aitNumber\\u003d20706|aitNumber\\u003d20706|aitNumber\\u003d20706|aitNumber\\u003d20743|aitNumber\\u003d20749|aitNumber\\u003d20764|aitNumber\\u003d20765|aitNumber\\u003d20782|aitNumber\\u003d20796|aitNumber\\u003d20827|aitNumber\\u003d20857|aitNumber\\u003d20867|aitNumber\\u003d20876|aitNumber\\u003d20892|aitNumber\\u003d20896|aitNumber\\u003d2089|aitNumber\\u003d20909|aitNumber\\u003d20914|aitNumber\\u003d20917|aitNumber\\u003d20918|aitNumber\\u003d20924|aitNumber\\u003d20924|aitNumber\\u003d20925|aitNumber\\u003d20929|aitNumber\\u003d20933|aitNumber\\u003d20933|aitNumber\\u003d2095|aitNumber\\u003d20973|aitNumber\\u003d21010|aitNumber\\u003d21035|aitNumber\\u003d21036|aitNumber\\u003d21050|aitNumber\\u003d21054|aitNumber\\u003d21060|aitNumber\\u003d21066|aitNumber\\u003d21072|aitNumber\\u003d21076|aitNumber\\u003d21085|aitNumber\\u003d21091|aitNumber\\u003d21135|aitNumber\\u003d21152|aitNumber\\u003d21156|aitNumber\\u003d21159|aitNumber\\u003d21160|aitNumber\\u003d21163|aitNumber\\u003d21166|aitNumber\\u003d21173|aitNumber\\u003d21173|aitNumber\\u003d21175|aitNumber\\u003d21176|aitNumber\\u003d21186|aitNumber\\u003d21195|aitNumber\\u003d21196|aitNumber\\u003d21203|aitNumber\\u003d21206|aitNumber\\u003d21214|aitNumber\\u003d21215|aitNumber\\u003d21226|aitNumber\\u003d21230|aitNumber\\u003d21239|aitNumber\\u003d21252|aitNumber\\u003d21277|aitNumber\\u003d21281|aitNumber\\u003d21287|aitNumber\\u003d21304|aitNumber\\u003d21305|aitNumber\\u003d21308|aitNumber\\u003d21310|aitNumber\\u003d21311|aitNumber\\u003d21312|aitNumber\\u003d21315|aitNumber\\u003d21317|aitNumber\\u003d21318|aitNumber\\u003d21369|aitNumber\\u003d21404|aitNumber\\u003d21514|aitNumber\\u003d21527|aitNumber\\u003d21597|aitNumber\\u003d21648|aitNumber\\u003d21659|aitNumber\\u003d21667|aitNumber\\u003d22034|aitNumber\\u003d22036|aitNumber\\u003d22174|aitNumber\\u003d22218|aitNumber\\u003d22255|aitNumber\\u003d22279|aitNumber\\u003d2227|aitNumber\\u003d22286|aitNumber\\u003d22359|aitNumber\\u003d22421|aitNumber\\u003d22525|aitNumber\\u003d22541|aitNumber\\u003d22602|aitNumber\\u003d22602|aitNumber\\u003d22764|aitNumber\\u003d22799|aitNumber\\u003d2316|aitNumber\\u003d23240|aitNumber\\u003d23245|aitNumber\\u003d23247|aitNumber\\u003d23324|aitNumber\\u003d23374|aitNumber\\u003d23384|aitNumber\\u003d2338|aitNumber\\u003d23397|aitNumber\\u003d23415|aitNumber\\u003d23427|aitNumber\\u003d23439|aitNumber\\u003d23449|aitNumber\\u003d23450|aitNumber\\u003d23451|aitNumber\\u003d23466|aitNumber\\u003d23476|aitNumber\\u003d23485|aitNumber\\u003d23493|aitNumber\\u003d23513|aitNumber\\u003d23513|aitNumber\\u003d23536|aitNumber\\u003d23541|aitNumber\\u003d23546|aitNumber\\u003d23558|aitNumber\\u003d23568|aitNumber\\u003d23572|aitNumber\\u003d23573|aitNumber\\u003d23582|aitNumber\\u003d23582|aitNumber\\u003d23582|aitNumber\\u003d23595|aitNumber\\u003d23597|aitNumber\\u003d23600|aitNumber\\u003d23615|aitNumber\\u003d23624|aitNumber\\u003d23663|aitNumber\\u003d23693|aitNumber\\u003d23695|aitNumber\\u003d23707|aitNumber\\u003d23728|aitNumber\\u003d23729|aitNumber\\u003d23731|aitNumber\\u003d23736|aitNumber\\u003d23755|aitNumber\\u003d23756|aitNumber\\u003d23758|aitNumber\\u003d23763|aitNumber\\u003d23784|aitNumber\\u003d23799|aitNumber\\u003d23802|aitNumber\\u003d23817|aitNumber\\u003d23826|aitNumber\\u003d23828|aitNumber\\u003d23837|aitNumber\\u003d23854|aitNumber\\u003d23859|aitNumber\\u003d23878|aitNumber\\u003d23888|aitNumber\\u003d23971|aitNumber\\u003d23989|aitNumber\\u003d23995|aitNumber\\u003d24005|aitNumber\\u003d24007|aitNumber\\u003d24024|aitNumber\\u003d24030|aitNumber\\u003d24057|aitNumber\\u003d24060|aitNumber\\u003d24073|aitNumber\\u003d24081|aitNumber\\u003d24089|aitNumber\\u003d24106|aitNumber\\u003d24134|aitNumber\\u003d24135|aitNumber\\u003d24137|aitNumber\\u003d24137|aitNumber\\u003d24139|aitNumber\\u003d24147|aitNumber\\u003d24156|aitNumber\\u003d24168|aitNumber\\u003d24182|aitNumber\\u003d24185|aitNumber\\u003d24199|aitNumber\\u003d2419|aitNumber\\u003d24202|aitNumber\\u003d24270|aitNumber\\u003d24270|aitNumber\\u003d24296|aitNumber\\u003d24302|aitNumber\\u003d24306|aitNumber\\u003d24345|aitNumber\\u003d24371|aitNumber\\u003d24378|aitNumber\\u003d24406|aitNumber\\u003d24418|aitNumber\\u003d24422|aitNumber\\u003d24476|aitNumber\\u003d24479|aitNumber\\u003d24486|aitNumber\\u003d24495|aitNumber\\u003d24503|aitNumber\\u003d24511|aitNumber\\u003d24553|aitNumber\\u003d24601|aitNumber\\u003d24606|aitNumber\\u003d24619|aitNumber\\u003d24652|aitNumber\\u003d24658|aitNumber\\u003d24667|aitNumber\\u003d24687|aitNumber\\u003d24707|aitNumber\\u003d24722|aitNumber\\u003d24724|aitNumber\\u003d24748|aitNumber\\u003d24761|aitNumber\\u003d24766|aitNumber\\u003d24767|aitNumber\\u003d24780|aitNumber\\u003d24790|aitNumber\\u003d24827|aitNumber\\u003d24857|aitNumber\\u003d24875|aitNumber\\u003d24894|aitNumber\\u003d24894|aitNumber\\u003d24895|aitNumber\\u003d24903|aitNumber\\u003d24929|aitNumber\\u003d24937|aitNumber\\u003d24971|aitNumber\\u003d25005|aitNumber\\u003d25027|aitNumber\\u003d25029|aitNumber\\u003d25042|aitNumber\\u003d25044|aitNumber\\u003d25044|aitNumber\\u003d25049|aitNumber\\u003d25056|aitNumber\\u003d25064|aitNumber\\u003d25070|aitNumber\\u003d25085|aitNumber\\u003d25109|aitNumber\\u003d25171|aitNumber\\u003d25184|aitNumber\\u003d25193|aitNumber\\u003d25220|aitNumber\\u003d25241|aitNumber\\u003d25268|aitNumber\\u003d25304|aitNumber\\u003d25363|aitNumber\\u003d25398|aitNumber\\u003d25398|aitNumber\\u003d25421|aitNumber\\u003d25446|aitNumber\\u003d25465|aitNumber\\u003d25476|aitNumber\\u003d25493|aitNumber\\u003d25502|aitNumber\\u003d25507|aitNumber\\u003d25524|aitNumber\\u003d25529|aitNumber\\u003d25533|aitNumber\\u003d25538|aitNumber\\u003d25539|aitNumber\\u003d25569|aitNumber\\u003d25576|aitNumber\\u003d25588|aitNumber\\u003d25598|aitNumber\\u003d25605|aitNumber\\u003d25633|aitNumber\\u003d25641|aitNumber\\u003d25667|aitNumber\\u003d25731|aitNumber\\u003d25813|aitNumber\\u003d25834|aitNumber\\u003d25854|aitNumber\\u003d25880|aitNumber\\u003d25899|aitNumber\\u003d25975|aitNumber\\u003d25988|aitNumber\\u003d25991|aitNumber\\u003d26106|aitNumber\\u003d26107|aitNumber\\u003d26115|aitNumber\\u003d26117|aitNumber\\u003d26172|aitNumber\\u003d26173|aitNumber\\u003d26195|aitNumber\\u003d26199|aitNumber\\u003d26205|aitNumber\\u003d26217|aitNumber\\u003d26270|aitNumber\\u003d26280|aitNumber\\u003d26284|aitNumber\\u003d26342|aitNumber\\u003d26353|aitNumber\\u003d26399|aitNumber\\u003d26451|aitNumber\\u003d26457|aitNumber\\u003d26482|aitNumber\\u003d26495|aitNumber\\u003d26560|aitNumber\\u003d26595|aitNumber\\u003d26648|aitNumber\\u003d26649|aitNumber\\u003d26667|aitNumber\\u003d26669|aitNumber\\u003d26685|aitNumber\\u003d26712|aitNumber\\u003d26719|aitNumber\\u003d26833|aitNumber\\u003d26932|aitNumber\\u003d26970|aitNumber\\u003d26970|aitNumber\\u003d27056|aitNumber\\u003d27224|aitNumber\\u003d27436|aitNumber\\u003d27679|aitNumber\\u003d27920|aitNumber\\u003d2803|aitNumber\\u003d28140|aitNumber\\u003d28209|aitNumber\\u003d28526|aitNumber\\u003d28687|aitNumber\\u003d28688|aitNumber\\u003d28708|aitNumber\\u003d28718|aitNumber\\u003d28727|aitNumber\\u003d28795|aitNumber\\u003d28840|aitNumber\\u003d29188|aitNumber\\u003d29209|aitNumber\\u003d29387|aitNumber\\u003d29412|aitNumber\\u003d29508|aitNumber\\u003d29549|aitNumber\\u003d2976|aitNumber\\u003d29787|aitNumber\\u003d29904|aitNumber\\u003d30067|aitNumber\\u003d30219|aitNumber\\u003d30620|aitNumber\\u003d306|aitNumber\\u003d30726|aitNumber\\u003d31138|aitNumber\\u003d31159|aitNumber\\u003d31265|aitNumber\\u003d31347|aitNumber\\u003d3252|aitNumber\\u003d3252|aitNumber\\u003d32589|aitNumber\\u003d32657|aitNumber\\u003d32664|aitNumber\\u003d32758|aitNumber\\u003d32759|aitNumber\\u003d32910|aitNumber\\u003d33810|aitNumber\\u003d34310|aitNumber\\u003d34381|aitNumber\\u003d34397|aitNumber\\u003d352|aitNumber\\u003d35587|aitNumber\\u003d35655|aitNumber\\u003d35857|aitNumber\\u003d36223|aitNumber\\u003d36732|aitNumber\\u003d36732|aitNumber\\u003d3684|aitNumber\\u003d36998|aitNumber\\u003d37003|aitNumber\\u003d37004|aitNumber\\u003d37076|aitNumber\\u003d37077|aitNumber\\u003d3711|aitNumber\\u003d37338|aitNumber\\u003d37378|aitNumber\\u003d3781|aitNumber\\u003d37852|aitNumber\\u003d37938|aitNumber\\u003d38087|aitNumber\\u003d383|aitNumber\\u003d383|aitNumber\\u003d38420|aitNumber\\u003d38548|aitNumber\\u003d38637|aitNumber\\u003d38791|aitNumber\\u003d39057|aitNumber\\u003d39147|aitNumber\\u003d39182|aitNumber\\u003d39432|aitNumber\\u003d39433|aitNumber\\u003d39889|aitNumber\\u003d39892|aitNumber\\u003d39892|aitNumber\\u003d39977|aitNumber\\u003d40168|aitNumber\\u003d40198|aitNumber\\u003d40208|aitNumber\\u003d4025|aitNumber\\u003d4025|aitNumber\\u003d40425|aitNumber\\u003d40472|aitNumber\\u003d40509|aitNumber\\u003d40701|aitNumber\\u003d40757|aitNumber\\u003d4075|aitNumber\\u003d40856|aitNumber\\u003d40897|aitNumber\\u003d40909|aitNumber\\u003d41023|aitNumber\\u003d41094|aitNumber\\u003d4114|aitNumber\\u003d4114|aitNumber\\u003d41268|aitNumber\\u003d4152|aitNumber\\u003d41707|aitNumber\\u003d41821|aitNumber\\u003d41923|aitNumber\\u003d4204|aitNumber\\u003d4208|aitNumber\\u003d4213|aitNumber\\u003d42393|aitNumber\\u003d42852|aitNumber\\u003d42919|aitNumber\\u003d43189|aitNumber\\u003d43499|aitNumber\\u003d43809|aitNumber\\u003d43889|aitNumber\\u003d43989|aitNumber\\u003d44179|aitNumber\\u003d44679|aitNumber\\u003d44679|aitNumber\\u003d45099|aitNumber\\u003d45222|aitNumber\\u003d45224|aitNumber\\u003d45228|aitNumber\\u003d45411|aitNumber\\u003d454|aitNumber\\u003d4562|aitNumber\\u003d45631|aitNumber\\u003d45691|aitNumber\\u003d45703|aitNumber\\u003d45851|aitNumber\\u003d4601|aitNumber\\u003d4603|aitNumber\\u003d46141|aitNumber\\u003d46252|aitNumber\\u003d46263|aitNumber\\u003d46401|aitNumber\\u003d46572|aitNumber\\u003d46611|aitNumber\\u003d46711|aitNumber\\u003d46901|aitNumber\\u003d46911|aitNumber\\u003d469|aitNumber\\u003d47021|aitNumber\\u003d47061|aitNumber\\u003d47152|aitNumber\\u003d47191|aitNumber\\u003d47231|aitNumber\\u003d47401|aitNumber\\u003d47581|aitNumber\\u003d47591|aitNumber\\u003d47791|aitNumber\\u003d48001|aitNumber\\u003d48051|aitNumber\\u003d48082|aitNumber\\u003d48161|aitNumber\\u003d48172|aitNumber\\u003d48191|aitNumber\\u003d48261|aitNumber\\u003d48291|aitNumber\\u003d48291|aitNumber\\u003d48311|aitNumber\\u003d48351|aitNumber\\u003d48351|aitNumber\\u003d48351|aitNumber\\u003d48352|aitNumber\\u003d48472|aitNumber\\u003d48472|aitNumber\\u003d48521|aitNumber\\u003d48561|aitNumber\\u003d48842|aitNumber\\u003d48842|aitNumber\\u003d48883|aitNumber\\u003d48951|aitNumber\\u003d48971|aitNumber\\u003d49061|aitNumber\\u003d49111|aitNumber\\u003d49153|aitNumber\\u003d49301|aitNumber\\u003d4970|aitNumber\\u003d4973|aitNumber\\u003d49741|aitNumber\\u003d49742|aitNumber\\u003d4974|aitNumber\\u003d49892|aitNumber\\u003d49902|aitNumber\\u003d50072|aitNumber\\u003d50073|aitNumber\\u003d50141|aitNumber\\u003d50161|aitNumber\\u003d50231|aitNumber\\u003d50491|aitNumber\\u003d50691|aitNumber\\u003d50763|aitNumber\\u003d50781|aitNumber\\u003d50901|aitNumber\\u003d51091|aitNumber\\u003d51141|aitNumber\\u003d51151|aitNumber\\u003d51322|aitNumber\\u003d51351|aitNumber\\u003d51391|aitNumber\\u003d51421|aitNumber\\u003d51492|aitNumber\\u003d51672|aitNumber\\u003d51831|aitNumber\\u003d52221|aitNumber\\u003d52241|aitNumber\\u003d52281|aitNumber\\u003d52281|aitNumber\\u003d52331|aitNumber\\u003d52351|aitNumber\\u003d52563|aitNumber\\u003d52632|aitNumber\\u003d52701|aitNumber\\u003d52782|aitNumber\\u003d53202|aitNumber\\u003d53281|aitNumber\\u003d53311|aitNumber\\u003d53371|aitNumber\\u003d53391|aitNumber\\u003d53411|aitNumber\\u003d53421|aitNumber\\u003d53531|aitNumber\\u003d53561|aitNumber\\u003d53611|aitNumber\\u003d53681|aitNumber\\u003d53681|aitNumber\\u003d53711|aitNumber\\u003d53761|aitNumber\\u003d53892|aitNumber\\u003d53931|aitNumber\\u003d54021|aitNumber\\u003d54032|aitNumber\\u003d54082|aitNumber\\u003d54191|aitNumber\\u003d54221|aitNumber\\u003d54231|aitNumber\\u003d54581|aitNumber\\u003d54601|aitNumber\\u003d5466|aitNumber\\u003d54782|aitNumber\\u003d54791|aitNumber\\u003d54881|aitNumber\\u003d55031|aitNumber\\u003d55091|aitNumber\\u003d5518|aitNumber\\u003d55231|aitNumber\\u003d55251|aitNumber\\u003d55371|aitNumber\\u003d55541|aitNumber\\u003d55821|aitNumber\\u003d55851|aitNumber\\u003d55921|aitNumber\\u003d55951|aitNumber\\u003d56401|aitNumber\\u003d56462|aitNumber\\u003d56601|aitNumber\\u003d56612|aitNumber\\u003d56851|aitNumber\\u003d56871|aitNumber\\u003d56951|aitNumber\\u003d56|aitNumber\\u003d57061|aitNumber\\u003d57132|aitNumber\\u003d57222|aitNumber\\u003d57552|aitNumber\\u003d57561|aitNumber\\u003d5756|aitNumber\\u003d57661|aitNumber\\u003d57732|aitNumber\\u003d57754|aitNumber\\u003d57771|aitNumber\\u003d57911|aitNumber\\u003d5801|aitNumber\\u003d58131|aitNumber\\u003d58204|aitNumber\\u003d5835|aitNumber\\u003d58413|aitNumber\\u003d58441|aitNumber\\u003d58611|aitNumber\\u003d5874|aitNumber\\u003d58811|aitNumber\\u003d58861|aitNumber\\u003d5894|aitNumber\\u003d58951|aitNumber\\u003d58962|aitNumber\\u003d58965|aitNumber\\u003d59315|aitNumber\\u003d59335|aitNumber\\u003d59396|aitNumber\\u003d59485|aitNumber\\u003d5950|aitNumber\\u003d59655|aitNumber\\u003d59656|aitNumber\\u003d59915|aitNumber\\u003d59925|aitNumber\\u003d60035|aitNumber\\u003d60115|aitNumber\\u003d60176|aitNumber\\u003d60195|aitNumber\\u003d60245|aitNumber\\u003d60255|aitNumber\\u003d60265|aitNumber\\u003d60266|aitNumber\\u003d60267|aitNumber\\u003d60485|aitNumber\\u003d60486|aitNumber\\u003d60487|aitNumber\\u003d60515|aitNumber\\u003d60525|aitNumber\\u003d60565|aitNumber\\u003d60595|aitNumber\\u003d60611|aitNumber\\u003d60696|aitNumber\\u003d60698|aitNumber\\u003d6071|aitNumber\\u003d60755|aitNumber\\u003d60805|aitNumber\\u003d60876|aitNumber\\u003d60987|aitNumber\\u003d60988|aitNumber\\u003d6104|aitNumber\\u003d61215|aitNumber\\u003d61266|aitNumber\\u003d61275|aitNumber\\u003d61365|aitNumber\\u003d61435|aitNumber\\u003d61585|aitNumber\\u003d61625|aitNumber\\u003d61635|aitNumber\\u003d61677|aitNumber\\u003d61818|aitNumber\\u003d61855|aitNumber\\u003d61888|aitNumber\\u003d61889|aitNumber\\u003d61895|aitNumber\\u003d61897|aitNumber\\u003d61965|aitNumber\\u003d61965|aitNumber\\u003d62095|aitNumber\\u003d62285|aitNumber\\u003d62465|aitNumber\\u003d62716|aitNumber\\u003d62736|aitNumber\\u003d62751|aitNumber\\u003d62895|aitNumber\\u003d62956|aitNumber\\u003d63066|aitNumber\\u003d63105|aitNumber\\u003d6317|aitNumber\\u003d63285|aitNumber\\u003d63325|aitNumber\\u003d6335|aitNumber\\u003d63437|aitNumber\\u003d63505|aitNumber\\u003d63506|aitNumber\\u003d63526|aitNumber\\u003d63566|aitNumber\\u003d63567|aitNumber\\u003d63598|aitNumber\\u003d63786|aitNumber\\u003d63816|aitNumber\\u003d63816|aitNumber\\u003d63905|aitNumber\\u003d64035|aitNumber\\u003d64105|aitNumber\\u003d64198|aitNumber\\u003d64276|aitNumber\\u003d6433|aitNumber\\u003d64386|aitNumber\\u003d64397|aitNumber\\u003d64445|aitNumber\\u003d64448|aitNumber\\u003d64545|aitNumber\\u003d64586|aitNumber\\u003d64586|aitNumber\\u003d64897|aitNumber\\u003d64899|aitNumber\\u003d65106|aitNumber\\u003d65115|aitNumber\\u003d65135|aitNumber\\u003d65165|aitNumber\\u003d6516|aitNumber\\u003d65173|aitNumber\\u003d65275|aitNumber\\u003d65415|aitNumber\\u003d65416|aitNumber\\u003d65436|aitNumber\\u003d65507|aitNumber\\u003d66220|aitNumber\\u003d66220|aitNumber\\u003d66245|aitNumber\\u003d66337|aitNumber\\u003d66458|aitNumber\\u003d66476|aitNumber\\u003d66505|aitNumber\\u003d67086|aitNumber\\u003d67195|aitNumber\\u003d67235|aitNumber\\u003d67325|aitNumber\\u003d6734|aitNumber\\u003d67475|aitNumber\\u003d67617|aitNumber\\u003d67617|aitNumber\\u003d676|aitNumber\\u003d67815|aitNumber\\u003d68025|aitNumber\\u003d68125|aitNumber\\u003d68125|aitNumber\\u003d68335|aitNumber\\u003d6841|aitNumber\\u003d68665|aitNumber\\u003d68747|aitNumber\\u003d68815|aitNumber\\u003d68816|aitNumber\\u003d68987|aitNumber\\u003d68998|aitNumber\\u003d69278|aitNumber\\u003d69278|aitNumber\\u003d69279|aitNumber\\u003d69335|aitNumber\\u003d69355|aitNumber\\u003d69375|aitNumber\\u003d69420|aitNumber\\u003d69437|aitNumber\\u003d69590|aitNumber\\u003d69590|aitNumber\\u003d69599|aitNumber\\u003d69599|aitNumber\\u003d69603|aitNumber\\u003d69624|aitNumber\\u003d69625|aitNumber\\u003d69626|aitNumber\\u003d69642|aitNumber\\u003d69652|aitNumber\\u003d69661|aitNumber\\u003d69692|aitNumber\\u003d69692|aitNumber\\u003d69712|aitNumber\\u003d69714|aitNumber\\u003d69744|aitNumber\\u003d69755|aitNumber\\u003d69765|aitNumber\\u003d69772|aitNumber\\u003d69793|aitNumber\\u003d69802|aitNumber\\u003d69819|aitNumber\\u003d69820|aitNumber\\u003d69838|aitNumber\\u003d69840|aitNumber\\u003d69850|aitNumber\\u003d69850|aitNumber\\u003d69858|aitNumber\\u003d69872|aitNumber\\u003d69880|aitNumber\\u003d69904|aitNumber\\u003d69913|aitNumber\\u003d69917|aitNumber\\u003d69960|aitNumber\\u003d69960|aitNumber\\u003d69965|aitNumber\\u003d69966|aitNumber\\u003d69969|aitNumber\\u003d69971|aitNumber\\u003d69988|aitNumber\\u003d69992|aitNumber\\u003d69995|aitNumber\\u003d69999|aitNumber\\u003d70020|aitNumber\\u003d70035|aitNumber\\u003d70038|aitNumber\\u003d70040|aitNumber\\u003d70041|aitNumber\\u003d70043|aitNumber\\u003d70054|aitNumber\\u003d70055|aitNumber\\u003d70078|aitNumber\\u003d70081|aitNumber\\u003d70096|aitNumber\\u003d70102|aitNumber\\u003d70185|aitNumber\\u003d70185|aitNumber\\u003d70214|aitNumber\\u003d70228|aitNumber\\u003d70242|aitNumber\\u003d70249|aitNumber\\u003d70255|aitNumber\\u003d70280|aitNumber\\u003d70344|aitNumber\\u003d70351|aitNumber\\u003d70354|aitNumber\\u003d70365|aitNumber\\u003d70395|aitNumber\\u003d70395|aitNumber\\u003d70405|aitNumber\\u003d70406|aitNumber\\u003d70406|aitNumber\\u003d70407|aitNumber\\u003d70418|aitNumber\\u003d70419|aitNumber\\u003d70434|aitNumber\\u003d70438|aitNumber\\u003d70442|aitNumber\\u003d70447|aitNumber\\u003d70481|aitNumber\\u003d70483|aitNumber\\u003d70487|aitNumber\\u003d70488|aitNumber\\u003d70507|aitNumber\\u003d70507|aitNumber\\u003d70514|aitNumber\\u003d70515|aitNumber\\u003d70525|aitNumber\\u003d70529|aitNumber\\u003d70532|aitNumber\\u003d70533|aitNumber\\u003d70557|aitNumber\\u003d70563|aitNumber\\u003d70575|aitNumber\\u003d70581|aitNumber\\u003d70588|aitNumber\\u003d70591|aitNumber\\u003d70601|aitNumber\\u003d70611|aitNumber\\u003d70616|aitNumber\\u003d70639|aitNumber\\u003d70651|aitNumber\\u003d70653|aitNumber\\u003d70660|aitNumber\\u003d70686|aitNumber\\u003d70699|aitNumber\\u003d70700|aitNumber\\u003d70710|aitNumber\\u003d70730|aitNumber\\u003d70731|aitNumber\\u003d70735|aitNumber\\u003d70772|aitNumber\\u003d70793|aitNumber\\u003d70793|aitNumber\\u003d70819|aitNumber\\u003d70838|aitNumber\\u003d70845|aitNumber\\u003d70846|aitNumber\\u003d70848|aitNumber\\u003d70856|aitNumber\\u003d70857|aitNumber\\u003d70857|aitNumber\\u003d70862|aitNumber\\u003d70872|aitNumber\\u003d70890|aitNumber\\u003d70899|aitNumber\\u003d70900|aitNumber\\u003d70914|aitNumber\\u003d71004|aitNumber\\u003d71021|aitNumber\\u003d71027|aitNumber\\u003d71076|aitNumber\\u003d71091|aitNumber\\u003d71102|aitNumber\\u003d71125|aitNumber\\u003d71133|aitNumber\\u003d71158|aitNumber\\u003d71159|aitNumber\\u003d71160|aitNumber\\u003d71163|aitNumber\\u003d71164|aitNumber\\u003d71181|aitNumber\\u003d71192|aitNumber\\u003d71193|aitNumber\\u003d71201|aitNumber\\u003d71214|aitNumber\\u003d71229|aitNumber\\u003d71257|aitNumber\\u003d71262|aitNumber\\u003d71264|aitNumber\\u003d71277|aitNumber\\u003d71296|aitNumber\\u003d71330|aitNumber\\u003d71338|aitNumber\\u003d71341|aitNumber\\u003d71347|aitNumber\\u003d71377|aitNumber\\u003d7142|aitNumber\\u003d71442|aitNumber\\u003d71451|aitNumber\\u003d71454|aitNumber\\u003d71466|aitNumber\\u003d71497|aitNumber\\u003d71504|aitNumber\\u003d71505|aitNumber\\u003d71511|aitNumber\\u003d71515|aitNumber\\u003d71541|aitNumber\\u003d71551|aitNumber\\u003d71552|aitNumber\\u003d71610|aitNumber\\u003d71612|aitNumber\\u003d71630|aitNumber\\u003d71635|aitNumber\\u003d71636|aitNumber\\u003d71651|aitNumber\\u003d71689|aitNumber\\u003d71690|aitNumber\\u003d71691|aitNumber\\u003d71692|aitNumber\\u003d71693|aitNumber\\u003d71695|aitNumber\\u003d71697|aitNumber\\u003d71700|aitNumber\\u003d71701|aitNumber\\u003d71703|aitNumber\\u003d71751|aitNumber\\u003d71763|aitNumber\\u003d71764|aitNumber\\u003d71786|aitNumber\\u003d71787|aitNumber\\u003d71788|aitNumber\\u003d71813|aitNumber\\u003d71836|aitNumber\\u003d71842|aitNumber\\u003d71880|aitNumber\\u003d71896|aitNumber\\u003d71899|aitNumber\\u003d71949|aitNumber\\u003d71965|aitNumber\\u003d71983|aitNumber\\u003d72002|aitNumber\\u003d7446|aitNumber\\u003d7460|aitNumber\\u003d7485|aitNumber\\u003d7504|aitNumber\\u003d7519|aitNumber\\u003d7532|aitNumber\\u003d7534|aitNumber\\u003d7535|aitNumber\\u003d7584|aitNumber\\u003d7640|aitNumber\\u003d7653|aitNumber\\u003d7673|aitNumber\\u003d7677|aitNumber\\u003d7683|aitNumber\\u003d7696|aitNumber\\u003d7726|aitNumber\\u003d7768|aitNumber\\u003d7770|aitNumber\\u003d7925|aitNumber\\u003d8022|aitNumber\\u003d8156|aitNumber\\u003d8363|aitNumber\\u003d8421|aitNumber\\u003d855|aitNumber\\u003d8561|aitNumber\\u003d8581|aitNumber\\u003d8581|aitNumber\\u003d8638|aitNumber\\u003d8731|aitNumber\\u003d8733|aitNumber\\u003d8740|aitNumber\\u003d8754|aitNumber\\u003d8765|aitNumber\\u003d8794|aitNumber\\u003d8805|aitNumber\\u003d8868|aitNumber\\u003d8881|aitNumber\\u003d8892|aitNumber\\u003d8906|aitNumber\\u003d8938|aitNumber\\u003d8993|aitNumber\\u003d9005|aitNumber\\u003d906|aitNumber\\u003d9102|aitNumber\\u003d9134|aitNumber\\u003d9303|aitNumber\\u003d9347|aitNumber\\u003d9359|aitNumber\\u003d9537|aitNumber\\u003d956|aitNumber\\u003d95|aitNumber\\u003d9605|aitNumber\\u003d9710|aitNumber\\u003d9851|aitNumber\\u003d986\",\n"
                    + "      \"filterType\": \"vmsByTag\",\n" + "      \"isCaseSensitive\": false\n"
                    + "    }\n" + "  }],\n" + "  \"logicalOperator\": \"AND\"\n" + "}";

    /**
     * Use this "test" to rewrite entities saved as a string to binary format.
     * This makes it much (i.e. 10x) faster to load them afterwards for other tests.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Ignore
    public void testRewriteAsBinary() throws Exception {

        final String filePath = "/home/turbo/work/bugs/OM-62188/turbonomic-diags-2020-08-31-turbonomic.com-7.22.6-3865f64-_1598892466863/repository-786bf984b8-8t2cj-diags/live.realtime.source.entities";

        final BufferedReader reader =
                new BufferedReader(new FileReader(filePath));

        String outFile = "/home/turbo/work/bugs/OM-62188/turbonomic-diags-2020-08-31-turbonomic.com-7.22.6-3865f64-_1598892466863/repository-786bf984b8-8t2cj-diags/live.realtime.source.entities.binary";

        FileOutputStream fos = new FileOutputStream(outFile);

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        int cnt = 0;
        while (reader.ready()) {
            final TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), bldr);
            TopologyEntityDTO entity = bldr.build();

            entity.writeDelimitedTo(fos);
            if (cnt++ % 1000 == 0) {
                logger.info(cnt);
            }
        }
        fos.flush();
        fos.close();
    }

    private void doForEntity(String filePath, Consumer<TopologyEntityDTO> consumer) throws IOException {
        File file = new File(filePath);
        MutableInt lineCnt = new MutableInt(0);
        try (InputStream is = FileUtils.openInputStream(file)) {
            try {
                while (true) {
                    TopologyEntityDTO e = TopologyEntityDTO.parseDelimitedFrom(is);
                    if (e == null) {
                        break;
                    }
//                    e.getData().getEntitiesList().forEach(segment -> {
                        lineCnt.increment();
                        consumer.accept(e);
//                    });
                    logger.info("Processed {}", lineCnt);
                }
            } catch (IOException e) {
                // Noop.

            }
        }
    }

    /**
     * Test the size of a topology.
     *
     * @throws IOException If anything goes wrong.
     */
    @Test
    @Ignore
    public void testSize() throws IOException {
        SearchQuery.Builder bldr = SearchQuery.newBuilder();
        JsonFormat.parser().merge(BIG_QUERY_JSON, bldr);
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator);
        SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder = liveTopologyStore.newRealtimeSourceTopology(TopologyInfo.getDefaultInstance());

        doForEntity("/home/turbo/work/bugs/OM-62188/turbonomic-diags-2020-08-31-turbonomic.com-7.22.6-3865f64-_1598892466863/repository-786bf984b8-8t2cj-diags/live.realtime.source.entities.binary", entity -> {
            sourceRealtimeTopologyBuilder.addEntities(Collections.singletonList(entity));
        });

        SourceRealtimeTopology topology = sourceRealtimeTopologyBuilder.finish();
        SearchResolver<RepoGraphEntity> searchResolver = new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
        Stopwatch start = Stopwatch.createStarted();
        List<RepoGraphEntity> results = searchResolver.search(bldr.build(), topology.entityGraph())
                .collect(Collectors.toList());
        logger.info("{} to get {} results", start.elapsed(TimeUnit.MILLISECONDS), results.size());
        logger.info("FOO");

//        logger.info(MemoryMetricsManager.sizesAndCounts(topology));
    }

    @Test
    @Ignore
    public void testRealtimeSource() throws IOException {
        // Put the filename here.
        final String filePath = "/home/turbo/Work/topologies/bofa/may23/";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator);
        SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder = liveTopologyStore.newRealtimeSourceTopology(TopologyInfo.getDefaultInstance());
        int lineCnt = 0;
        Map<EntityType, MutableLong> countsByType = new HashMap<>();

        final Stopwatch stopwatch = Stopwatch.createUnstarted();

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        while (reader.ready()) {
            final TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), bldr);
            TopologyEntityDTO entity = bldr.build();
            countsByType.computeIfAbsent(EntityType.forNumber(entity.getEntityType()), k -> new MutableLong(0)).increment();
            stopwatch.start();
            sourceRealtimeTopologyBuilder.addEntities(Collections.singleton(entity));
            stopwatch.stop();
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                logger.info("Processed {}", lineCnt);
            }
        }

        logger.info(countsByType);
        stopwatch.start();
        sourceRealtimeTopologyBuilder.finish();
        stopwatch.stop();

//        logger.info("Total construction time: {}\n" +
//                "Size: {}",
//            stopwatch.elapsed(TimeUnit.MILLISECONDS),
//            FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        stopwatch.reset();
        stopwatch.start();
        final TopologyGraph<RepoGraphEntity> topologyGraph = liveTopologyStore.getSourceTopology()
                                                                       .get().entityGraph();
        topologyGraph.entities()
                .map(RepoGraphEntity::getTopologyEntity)
                .forEach(e -> cnt.increment());
        stopwatch.stop();
        logger.info("Took {} to de-compress {} entities", stopwatch.elapsed(TimeUnit.SECONDS), cnt.intValue());

        stopwatch.reset();
        stopwatch.start();
        globalSupplyChainCalculator.getSupplyChainNodes(topologyGraph, x -> true,
                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);
        stopwatch.stop();
        logger.info("Hybrid GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
//        logger.info("Size with global supply chain: {}", FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        stopwatch.reset();
        stopwatch.start();
        globalSupplyChainCalculator.getSupplyChainNodes(topologyGraph, e -> EnvironmentTypeUtil.match(e.getEnvironmentType(), EnvironmentType.CLOUD),
                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);
        stopwatch.stop();
        logger.info("Cloud GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset();
        stopwatch.start();
        globalSupplyChainCalculator.getSupplyChainNodes(topologyGraph, e -> EnvironmentTypeUtil.match(e.getEnvironmentType(), EnvironmentType.ON_PREM),
                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);
        stopwatch.stop();
        logger.info("On-prem GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    @Ignore
    public void testRealtimeProjected() throws IOException {
        // Put the filename here.
        final String filePath = "/Volumes/Workspace/topologies/bofa/may23/repo/live.topology.source.entities";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator);

        ProjectedTopologyBuilder ptbldr = liveTopologyStore.newProjectedTopology(1, TopologyInfo.getDefaultInstance());

        int lineCnt = 0;
        Map<EntityType, MutableLong> countsByType = new HashMap<>();

        Stopwatch watch = Stopwatch.createUnstarted();

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        while (reader.ready()) {
            TopologyEntityDTO.Builder eBldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), eBldr);
            ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(1)
                .setProjectedPriceIndex(2)
                .setEntity(eBldr)
                .build();

            countsByType.computeIfAbsent(EntityType.forNumber(eBldr.getEntityType()), k -> new MutableLong(0)).increment();
            watch.start();
            ptbldr.addEntities(Collections.singleton(entity));
            watch.stop();
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                logger.info("Processed {}", lineCnt);
            }
        }

        logger.info(countsByType);


        watch.start();
        ptbldr.finish();
        watch.stop();

//        logger.info("Construction time: {}\nSize: {}", watch.elapsed(TimeUnit.MILLISECONDS),
//            FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getProjectedTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        watch.reset();
        watch.start();
        liveTopologyStore.getProjectedTopology().get().getEntities(Collections.emptySet(), Collections.emptySet())
            .forEach(e -> cnt.increment());
        watch.stop();
        logger.info("Took {} to de-compress {} entities", watch.elapsed(TimeUnit.SECONDS), cnt.intValue());
    }
}