/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
package com.alibaba.csp.sentinel.slots.block.authority;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.slotchain.AbstractLinkedProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.spi.Spi;

import java.util.Map;
import java.util.Set;

/**
 * 根据来源进行黑白名单访问控制
 * A {@link ProcessorSlot} that dedicates to {@link AuthorityRule} checking.
 *
 * @author leyou
 * @author Eric Zhao
 */
@Spi(order = Constants.ORDER_AUTHORITY_SLOT)
public class AuthoritySlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node, int count, boolean prioritized, Object... args)
            throws Throwable {
        checkBlackWhiteAuthority(resourceWrapper, context);
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }

    //通过规则校验器对所有规则进行限流
    //主要判断规则limit app字段是否包括请求origin、并根据规则是黑名单或白名单进行控制
    void checkBlackWhiteAuthority(ResourceWrapper resource, Context context) throws AuthorityException {
        Map<String, Set<AuthorityRule>> authorityRules = AuthorityRuleManager.getAuthorityRules();

        if (authorityRules == null) {
            return;
        }

        Set<AuthorityRule> rules = authorityRules.get(resource.getName());
        if (rules == null) {
            return;
        }

        for (AuthorityRule rule : rules) {
            if (!AuthorityRuleChecker.passCheck(rule, context)) {
                throw new AuthorityException(context.getOrigin(), rule);
            }
        }
    }
}
