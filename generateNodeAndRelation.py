# !/usr/bin/env python
# -*- coding:utf-8 -*-
# 从freebase的三元组文件中生成节点文件和关系文件.
import re
import collections

pattern = re.compile(r"\"(.*)\"")


def get_attribute_from(entity_str, current_node_id):
    """
    通过一个Freebase的实体字符串生成相应的属性列表.
    :param entity_str: 实体字符串
    :param current_node_id  当前节点id
    :return: 属性列表.
    """
    attribute = list()
    attribute.append(current_node_id)
    prefix, mid = entity_str.split(":")
    attribute.append("prefix:" + prefix)
    attribute.append("mid:" + mid)
    return attribute


def handle_line(inline, current_node_id, relationship_file):
    """
    处理每行的信息.
    :param inline:三元组行
    :param current_node_id: 当前的点id
    :param relationship_file 关系文件
    :return: current_node_id, 以及关系文件的内容.
    """
    tokens = inline.split("\t")
    subject_node_id = -1
    object_node_id = -1

    if len(tokens) == 3:  # 判断是否是三元组
        object_str = tokens[2].strip()[:-1]

        # 首先判断这个三元组是加属性的三元组还是加关系的三元组, 然后根据不同情况处理结果.
        if object_str.startswith("\""):
            if tokens[0] not in mid2nodeId:
                attribute = get_attribute_from(tokens[0], current_node_id)
                m = pattern.match(object_str)
                attribute_name = tokens[1].split(':')[1]

                if m:
                    attribute.append(attribute_name + ':' + m.group(1))

                mid2nodeId[tokens[0]] = attribute
                subject_node_id = current_node_id
                current_node_id += 1
            else:
                attribute = mid2nodeId[tokens[0]]
                m = pattern.match(object_str)
                attribute_name = tokens[1].split(':')[1]

                if m:
                    mid2nodeId[tokens[0]].append(attribute_name + ':' + m.group(1))

                subject_node_id = attribute[0]
        else:
            if tokens[0] not in mid2nodeId:
                attribute = get_attribute_from(tokens[0], current_node_id)
                mid2nodeId[tokens[0]] = attribute
                subject_node_id = current_node_id
                current_node_id += 1
            else:
                subject_node_id = mid2nodeId[tokens[0]][0]

            if object_str not in mid2nodeId:
                attribute = get_attribute_from(object_str, current_node_id)
                mid2nodeId[object_str] = attribute
                object_node_id = current_node_id
                current_node_id += 1
            else:
                object_node_id = mid2nodeId[object_str][0]

        if subject_node_id != -1 and object_node_id != -1:
            relationship_file.write(str(subject_node_id) + '\t' + tokens[1] + '\t' + str(object_node_id) + '\n')
    return current_node_id


# mid2nodeId中包含了所有的属性.
mid2nodeId = collections.OrderedDict()
currentNodeId = 0
relation_path = "schema2.relationship"

with open("schema2.ttl", "r", encoding="utf-8") as ttl, open(relation_path, "w", encoding="utf-8") as relation:
    for line in ttl:
        currentNodeId = handle_line(line, currentNodeId, relation)

print("点数据读取完成.")

with open("schema2.node", "w", encoding="utf-8") as nodeFile:
    for attributes in mid2nodeId.values():
        line = str(attributes[0]) + '\t' + '\t'.join(attributes[1:]) + '\n'
        nodeFile.write(line)
