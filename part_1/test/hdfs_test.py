#!/usr/bin/env python3
import unittest
import pytest
from part_1 import hdfs

class TestHDFS(unittest.TestCase):
    pass

class TestNameNode(unittest.TestCase):
    def setUp(self):
        self.address = ("localhost", 7400)
        self.node = hdfs.NameNode(self.address)

    def test_address(self):
        self.assertEqual(self.node.address, self.address)
    
    def test_start(self):
        self.node.start()
        self.assertEqual(self.node.status, hdfs.NameNode.ALIVE)
        pass

    def test_add_data_node(self):
        data_node_address = ("localhost", 7678)
        self.node.add_data_node(data_node_address)

        item = self.node.data_nodes[data_node_address]
        self.assertIsNotNone(item)
        self.assertEqual(item['address'], data_node_address)
        self.assertEqual(item['size'], 500000000 )
        self.assertEqual(item['free_space'], 500000000)
        self.assertEqual(item['used_space'], 0)

    def test_add_dup_data_node(self):
        pass
    
    def test_del_data_node(self):
        self.assertEqual(self.node.data_nodes, {})
        data_node_address = ("localhost", 7678)
        self.node.add_data_node(data_node_address)
        
        item = self.node.data_nodes[data_node_address]
        self.assertIsNotNone(item)

        self.node.remove_data_node(data_node_address)
        self.assertEqual(len(self.node.data_nodes.keys()), 0)

        data_node_address_1 = ("localhost", 7678)
        self.node.add_data_node(data_node_address_1)

        data_node_address_2 = ("localhost", 7679)
        self.node.add_data_node(data_node_address_2)
        
        self.node.remove_data_node(data_node_address_2)
    
        # pytest.raises(KeyError, self.node.data_nodes[data_node_address_2])
        # self.assertEqual(len(self.node.data_nodes.keys()), 1)

if __name__=='__main__':
    unittest.main()