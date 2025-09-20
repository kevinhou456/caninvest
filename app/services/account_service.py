"""
账户服务 - 提供统一的账户列表获取和排序功能
"""

from typing import List, Optional
from app.models.account import Account
from app.models.family import Family


class AccountService:
    """账户服务类，提供统一的账户操作"""

    @staticmethod
    def get_accounts_display_list(family_id: Optional[int] = None) -> List[Account]:
        """
        获取按统一规则排序的账户显示列表

        排序规则：
        1. 首先按成员ID排序（与左侧导航栏顺序一致）
        2. 然后按账户类型排序（Regular, Margin, TFSA, RRSP, RESP, FHSA）
        3. 联合账户排在最后
        4. 最后按账户名称排序

        Args:
            family_id: 家庭ID，如果为None则自动获取第一个家庭

        Returns:
            List[Account]: 排序后的账户列表
        """
        # 获取家庭
        if family_id is None:
            family = Family.query.first()
            if not family:
                return []
            family_id = family.id

        # 获取所有账户
        accounts = Account.query.filter_by(family_id=family_id).all()

        # 定义账户类型排序顺序
        account_type_order = {
            'Regular': 1,
            'Margin': 2,
            'TFSA': 3,
            'RRSP': 4,
            'RESP': 5,
            'FHSA': 6
        }

        def get_account_sort_key(account):
            """获取账户排序键"""
            # 联合账户优先级最低，排在所有个人账户之后
            if account.is_joint:
                return (9999, 1000, account.name)

            # 获取账户成员信息
            account_members = account.account_members.all()

            # 获取主要成员或第一个成员的ID（用于匹配sidebar顺序）
            if account_members:
                primary_member = next((am.member for am in account_members if am.is_primary), None)
                if primary_member:
                    member_id = primary_member.id
                else:
                    member_id = account_members[0].member.id
            else:
                member_id = 9998  # 没有成员的账户排在倒数第二

            # 账户类型排序值
            account_type = account.account_type.name if account.account_type else ''
            type_order = account_type_order.get(account_type, 999)

            return (member_id, type_order, account.name)

        # 排序并返回
        return sorted(accounts, key=get_account_sort_key)

    @staticmethod
    def get_account_name_with_members(account: Account) -> str:
        """
        获取带成员信息的账户名称

        Args:
            account: 账户对象

        Returns:
            str: 带成员信息的账户名称
        """
        if not account.account_members:
            return account.name

        member_names = []
        for am in account.account_members:
            if am.is_primary:
                member_names.insert(0, am.member.name)
            else:
                member_names.append(am.member.name)

        if member_names:
            return f"{account.name} - {', '.join(member_names)}"

        return account.name

    @staticmethod
    def get_family_accounts(family_id: Optional[int] = None) -> List[Account]:
        """
        获取家庭的所有账户（不排序）

        Args:
            family_id: 家庭ID，如果为None则自动获取第一个家庭

        Returns:
            List[Account]: 账户列表
        """
        if family_id is None:
            family = Family.query.first()
            if not family:
                return []
            family_id = family.id

        return Account.query.filter_by(family_id=family_id).all()

    @staticmethod
    def get_account_ids_display_list(family_id: Optional[int] = None) -> List[int]:
        """
        获取按统一规则排序的账户ID列表

        Args:
            family_id: 家庭ID，如果为None则自动获取第一个家庭

        Returns:
            List[int]: 排序后的账户ID列表
        """
        accounts = AccountService.get_accounts_display_list(family_id)
        return [account.id for account in accounts]