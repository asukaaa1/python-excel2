"""
Mock iFood Data Generator - IMPROVED VERSION
Generates realistic sample data with benefits, discounts, and financial details
Based on actual iFood API structure
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List


class MockIFoodDataGenerator:
    """Generate mock iFood data for testing with complete financial details"""
    
    # Sample data for generating realistic content
    RESTAURANT_NAMES = [
        "Pizzaria Bella Napoli", "Burger House Premium", "Sushi Master",
        "Churrascaria Grill", "Cantina Italiana", "Comida Caseira da Vovó",
        "Padaria Pão Quente", "Açaí & Bowls", "Tacos Mexicanos", "Bistrô Francês"
    ]
    
    MANAGER_NAMES = [
        "João Silva", "Maria Santos", "Pedro Oliveira", "Ana Costa",
        "Carlos Ferreira", "Juliana Almeida", "Roberto Lima", "Patricia Souza"
    ]
    
    NEIGHBORHOODS = [
        "Centro", "Jardins", "Vila Madalena", "Pinheiros", "Moema",
        "Itaim Bibi", "Brooklin", "Morumbi", "Santana", "Tatuapé"
    ]
    
    FOOD_ITEMS = [
        "Pizza Margherita", "X-Burger", "Sushi Combo", "Picanha",
        "Espaguete à Carbonara", "Feijoada", "Pão de Queijo", "Açaí Bowl",
        "Tacos al Pastor", "Coq au Vin", "Lasagna", "Hot Dog Gourmet",
        "Temaki Salmão", "Marmitex", "Sanduíche Natural"
    ]
    
    @staticmethod
    def generate_merchant_data(merchant_id: str = None, num_orders: int = 100, days: int = 30) -> Dict:
        """
        Generate complete mock merchant data with orders
        
        Args:
            merchant_id: Optional merchant ID (generated if not provided)
            num_orders: Number of orders to generate
            days: Number of days back to generate orders for (default: 30)
            
        Returns:
            Dict with merchant details and orders
        """
        if not merchant_id:
            merchant_id = f"mock-{random.randint(1000, 9999)}"
        
        # Generate merchant details
        name = random.choice(MockIFoodDataGenerator.RESTAURANT_NAMES)
        manager = random.choice(MockIFoodDataGenerator.MANAGER_NAMES)
        neighborhood = random.choice(MockIFoodDataGenerator.NEIGHBORHOODS)
        
        # 30% chance of being an iFood Super restaurant
        is_super = random.random() < 0.8
        
        merchant_details = {
            'id': merchant_id,
            'name': name,
            'merchantManager': {'name': manager},
            'address': {'neighborhood': neighborhood},
            'status': 'ACTIVE',
            'isSuperRestaurant': is_super
        }
        
        # Generate orders
        orders = MockIFoodDataGenerator._generate_orders(num_orders, days)
        
        return {
            'details': merchant_details,
            'orders': orders
        }
    
    @staticmethod
    def _generate_orders(num_orders: int, days: int = 30) -> List[Dict]:
        """Generate a list of mock orders"""
        orders = []
        now = datetime.now()
        
        for i in range(num_orders):
            days_ago = random.randint(0, days)
            hours_offset = random.randint(0, 23)
            minutes_offset = random.randint(0, 59)
            
            order_date = now - timedelta(days=days_ago, hours=hours_offset, minutes=minutes_offset)
            order = MockIFoodDataGenerator._generate_single_order(order_date, i)
            orders.append(order)
        
        orders.sort(key=lambda x: x['createdAt'], reverse=True)
        return orders
    
    @staticmethod
    def _generate_single_order(order_date: datetime, order_num: int) -> Dict:
        """Generate a single mock order with complete financial data"""
        
        # Determine order status (85% concluded, 10% cancelled, 5% other)
        rand = random.random()
        if rand < 0.85:
            status = 'CONCLUDED'
        elif rand < 0.95:
            status = 'CANCELLED'
        else:
            status = random.choice(['PLACED', 'CONFIRMED', 'DISPATCHED'])
        
        # Generate cancellation data if cancelled
        cancellation_data = None
        if status == 'CANCELLED':
            cancellation_reasons = [
                "Ninguém poderá receber",
                "A forma de pagamento mudou",
                "Item indisponível",
                "Tempo de preparo muito longo",
                "Desistência do pedido"
            ]
            cancellation_origins = ["CONSUMER", "MERCHANT", "IFOOD"]
            cancellation_data = {
                "reason": random.choice(cancellation_reasons),
                "code": f"CANCEL_{random.randint(100, 999)}",
                "origin": random.choice(cancellation_origins)
            }
        
        # Generate feedback data if concluded (60% of orders get feedback)
        feedback_data = None
        if status == 'CONCLUDED' and random.random() < 0.6:
            # Compliments (positive feedback)
            compliments = []
            if random.random() < 0.7:  # 70% chance of compliments
                possible_compliments = [
                    "Qualidade de comida excelente",
                    "Satisfação em geral",
                    "Entrega rápida"
                ]
                num_compliments = random.randint(1, len(possible_compliments))
                compliments = random.sample(possible_compliments, num_compliments)
            
            # Complaints (negative feedback)
            complaints = []
            if random.random() < 0.3:  # 30% chance of complaints
                possible_complaints = [
                    "Embalagem danificada",
                    "Falta de item",
                    "Outro"
                ]
                num_complaints = random.randint(1, 2)
                complaints = random.sample(possible_complaints, num_complaints)
            
            feedback_data = {
                "rating": random.randint(3, 5) if not complaints else random.randint(1, 3),
                "compliments": compliments,
                "complaints": complaints
            }
        
        # Generate items
        num_items = random.randint(1, 5)
        items = []
        subtotal = 0
        
        for _ in range(num_items):
            item_name = random.choice(MockIFoodDataGenerator.FOOD_ITEMS)
            quantity = random.randint(1, 3)
            unit_price = round(random.uniform(15.0, 80.0), 2)
            item_total = round(quantity * unit_price, 2)
            
            items.append({
                'name': item_name,
                'quantity': quantity,
                'unitPrice': unit_price,
                'totalPrice': item_total
            })
            subtotal += item_total
        
        # Generate delivery fee
        delivery_fee = round(random.uniform(3.0, 15.0), 2)
        
        # Generate benefits/discounts (40% chance of having a discount)
        benefits = []
        total_benefit_value = 0
        
        if random.random() < 0.4 and status == 'CONCLUDED':
            # Item discount (10-30% of subtotal)
            item_discount = round(subtotal * random.uniform(0.05, 0.15), 2)
            ifood_subsidy = round(item_discount * random.uniform(0.5, 0.8), 2)
            merchant_subsidy = round(item_discount - ifood_subsidy, 2)
            
            benefits.append({
                'value': item_discount,
                'target': 'CART',
                'sponsorshipValues': [
                    {'name': 'IFOOD', 'value': ifood_subsidy},
                    {'name': 'MERCHANT', 'value': merchant_subsidy}
                ]
            })
            total_benefit_value += item_discount
        
        # Delivery fee discount (30% chance)
        if random.random() < 0.3 and status == 'CONCLUDED':
            delivery_discount = round(delivery_fee * random.uniform(0.5, 1.0), 2)
            benefits.append({
                'value': delivery_discount,
                'target': 'DELIVERY_FEE',
                'sponsorshipValues': [
                    {'name': 'IFOOD', 'value': delivery_discount}
                ]
            })
            total_benefit_value += delivery_discount
        
        # Calculate total
        total = round(subtotal + delivery_fee - total_benefit_value, 2)
        
        # Payment method (70% online, 30% cash)
        payment_method = 'CREDIT' if random.random() < 0.7 else 'CASH'
        payment_liability = 'ONLINE' if payment_method in ['CREDIT', 'DEBIT', 'PIX'] else 'MERCHANT'
        
        # Customer info
        customer_id = f"customer-{random.randint(1000, 9999)}"
        is_new_customer = random.random() < 0.3  # 30% are new customers
        
        # Build order object
        order = {
            'id': f"order-{order_num:06d}",
            'displayId': f"#{random.randint(1000, 9999)}",
            'createdAt': order_date.isoformat(),
            'orderStatus': status,
            'totalPrice': total if status == 'CONCLUDED' else 0,
            'deliveryFee': delivery_fee,
            'total': {
                'orderAmount': total if status == 'CONCLUDED' else 0,
                'subTotal': subtotal,
                'deliveryFee': delivery_fee,
                'benefits': total_benefit_value
            },
            'benefits': benefits,
            'payment': {
                'method': payment_method,
                'liability': payment_liability
            },
            'items': items,
            'customer': {
                'id': customer_id,
                'name': f"Cliente {customer_id[-4:]}",
                'isNewCustomer': is_new_customer
            },
            'platform': 'iFood',
            'orderType': random.choice(['DELIVERY', 'INDOOR', 'TAKEOUT'])
        }
        
        # Add cancellation data if applicable
        if cancellation_data:
            order['cancellation'] = cancellation_data
        
        # Add feedback data if applicable
        if feedback_data:
            order['feedback'] = feedback_data
        
        return order
    
    @staticmethod
    def generate_financial_data(start_date: str, end_date: str) -> Dict:
        """Generate mock financial data"""
        try:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
            days = (end - start).days + 1
        except:
            days = 30
        
        daily_revenue = round(random.uniform(800, 2500), 2) * days
        daily_orders = random.randint(15, 60) * days
        
        return {
            'period': {
                'start': start_date,
                'end': end_date
            },
            'revenue': {
                'total': daily_revenue,
                'average_per_day': round(daily_revenue / days, 2)
            },
            'orders': {
                'total': daily_orders,
                'average_per_day': round(daily_orders / days, 1)
            },
            'average_ticket': round(daily_revenue / daily_orders, 2) if daily_orders > 0 else 0
        }
    
    @staticmethod
    def generate_interruption(hours_ago: int = 0, duration_hours: int = 2) -> Dict:
        """Generate a mock interruption"""
        now = datetime.now()
        start = now - timedelta(hours=hours_ago)
        end = start + timedelta(hours=duration_hours)
        
        reasons = [
            "Manutenção programada",
            "Falta de energia",
            "Problema técnico",
            "Pausa para reabastecimento",
            "Treinamento de equipe"
        ]
        
        return {
            'id': f"interruption-{random.randint(1000, 9999)}",
            'description': random.choice(reasons),
            'start': start.isoformat(),
            'end': end.isoformat()
        }
    
    @staticmethod
    def generate_test_dataset() -> Dict:
        """Generate a complete test dataset with multiple restaurants"""
        restaurants = []
        
        for i in range(5):
            merchant_id = f"mock-restaurant-{i+1}"
            data = MockIFoodDataGenerator.generate_merchant_data(
                merchant_id=merchant_id,
                num_orders=random.randint(80, 150)
            )
            restaurants.append(data)
        
        return {
            'restaurants': restaurants,
            'generated_at': datetime.now().isoformat(),
            'note': 'This is mock data for testing purposes'
        }


if __name__ == "__main__":
    print("Mock iFood Data Generator - Improved Version")
    print("=" * 60)
    
    # Generate sample data
    print("\nGenerating sample restaurant data...")
    data = MockIFoodDataGenerator.generate_merchant_data(num_orders=20)
    
    print(f"\nMerchant: {data['details']['name']}")
    print(f"Manager: {data['details']['merchantManager']['name']}")
    print(f"Orders: {len(data['orders'])}")
    
    concluded = [o for o in data['orders'] if o['orderStatus'] == 'CONCLUDED']
    total_revenue = sum(o['totalPrice'] for o in concluded)
    total_benefits = sum(o['total'].get('benefits', 0) for o in concluded)
    new_customers = sum(1 for o in concluded if o['customer'].get('isNewCustomer'))
    
    print(f"Concluded Orders: {len(concluded)}")
    print(f"Total Revenue: R$ {total_revenue:,.2f}")
    print(f"Total Discounts: R$ {total_benefits:,.2f}")
    print(f"New Customers: {new_customers}")
    
    print("\n✅ Mock data generation working correctly!")